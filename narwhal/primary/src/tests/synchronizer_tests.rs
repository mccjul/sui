// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{common::create_db_stores, synchronizer::Synchronizer};
use consensus::{dag::Dag, metrics::ConsensusMetrics};
use fastcrypto::{hash::Hash, traits::KeyPair};
use prometheus::Registry;
use tokio::sync::watch;
use std::{collections::BTreeSet, sync::Arc};
use test_utils::{make_optimal_signed_certificates, CommitteeFixture};
use types::Certificate;

#[tokio::test]
async fn deliver_certificate_using_dag() {
    let fixture = CommitteeFixture::builder().build();
    let name = fixture.authorities().next().unwrap().public_key();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();

    let (_, certificates_store, payload_store) = create_db_stores();
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus, rx_consensus) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));
    let dag = Arc::new(Dag::new(&committee, rx_consensus, consensus_metrics).1);

    let mut synchronizer = Synchronizer::new(
        name,
        fixture.committee().into(),
        worker_cache.clone(),
        certificates_store,
        payload_store,
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        Some(dag.clone()),
    );

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let keys = fixture
        .authorities()
        .map(|a| a.keypair().copy())
        .take(3)
        .collect::<Vec<_>>();
    let (mut certificates, _next_parents) =
        make_optimal_signed_certificates(1..=4, &genesis, &committee, &keys);

    // insert the certificates in the DAG
    for certificate in certificates.clone() {
        dag.insert(certificate).await.unwrap();
    }

    // take the last one (top) and test for parents
    let test_certificate = certificates.pop_back().unwrap();

    // ensure that the certificate parents are found
    let parents_available = synchronizer.check_parents(&test_certificate).await.unwrap();
    assert!(parents_available);
}

#[tokio::test]
async fn deliver_certificate_using_store() {
    let fixture = CommitteeFixture::builder().build();
    let name = fixture.authorities().next().unwrap().public_key();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();

    let (_, certificates_store, payload_store) = create_db_stores();
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let mut synchronizer = Synchronizer::new(
        name,
        fixture.committee().into(),
        worker_cache.clone(),
        certificates_store.clone(),
        payload_store.clone(),
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        None,
    );

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let keys = fixture
        .authorities()
        .map(|a| a.keypair().copy())
        .take(3)
        .collect::<Vec<_>>();
    let (mut certificates, _next_parents) =
        make_optimal_signed_certificates(1..=4, &genesis, &committee, &keys);

    // insert the certificates in the DAG
    for certificate in certificates.clone() {
        certificates_store.write(certificate).unwrap();
    }

    // take the last one (top) and test for parents
    let test_certificate = certificates.pop_back().unwrap();

    // ensure that the certificate parents are found
    let parents_available = synchronizer.check_parents(&test_certificate).await.unwrap();
    assert!(parents_available);
}

#[tokio::test]
async fn deliver_certificate_not_found_parents() {
    let fixture = CommitteeFixture::builder().build();
    let name = fixture.authorities().next().unwrap().public_key();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();

    let (_, certificates_store, payload_store) = create_db_stores();
    let (tx_certificate_waiter, mut rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let mut synchronizer = Synchronizer::new(
        name,
        fixture.committee().into(),
        worker_cache.clone(),
        certificates_store,
        payload_store,
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        None,
    );

    // create some certificates in a complete DAG form
    let genesis_certs = Certificate::genesis(&committee);
    let genesis = genesis_certs
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let keys = fixture
        .authorities()
        .map(|a| a.keypair().copy())
        .take(3)
        .collect::<Vec<_>>();
    let (mut certificates, _next_parents) =
        make_optimal_signed_certificates(1..=4, &genesis, &committee, &keys);

    // take the last one (top) and test for parents
    let test_certificate = certificates.pop_back().unwrap();

    // we try to find the certificate's parents
    let parents_available = synchronizer.check_parents(&test_certificate).await.unwrap();

    // and we should fail
    assert!(!parents_available);

    let certificate = rx_certificate_waiter.recv().await.unwrap();

    assert_eq!(certificate, test_certificate);
}

/*
// TODO-DNS copied from HeaderWaiter
async fn successfully_synchronize_batches() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let author = fixture.authorities().next().unwrap();
    let primary = fixture.authorities().nth(1).unwrap();
    let name = primary.public_key();
    let (_, certificate_store, payload_store) = create_db_stores();
    let gc_depth: Round = 1;
    let (_tx_reconfigure, rx_reconfigure) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_synchronizer, rx_synchronizer) = test_utils::test_channel!(10);
    let (tx_core, mut rx_core) = test_utils::test_channel!(10);
    let (tx_primary_messages, _rx_primary_messages) = test_utils::test_channel!(1000);
    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let own_address = network::multiaddr_to_address(&committee.primary(&name).unwrap()).unwrap();

    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(primary.network_keypair().copy().private().0.to_bytes())
        .start(anemo::Router::new())
        .unwrap();

    let _header_waiter_handle = HeaderWaiter::spawn(
        name.clone(),
        committee.clone(),
        worker_cache.clone(),
        certificate_store,
        payload_store.clone(),
        rx_consensus_round_updates,
        gc_depth,
        rx_reconfigure,
        rx_synchronizer,
        tx_core,
        tx_primary_messages,
        metrics,
        P2pNetwork::new(network.clone()),
    );

    // AND spin up a worker node that primary owns
    let worker_id = 0;
    let worker = primary.worker(worker_id);
    let worker_keypair = worker.keypair();
    let worker_name = worker_keypair.public().to_owned();
    let worker_address = &worker.info().worker_address;

    let handle = worker_listener(1, worker_address.clone(), worker_keypair);
    let address = network::multiaddr_to_address(worker_address).unwrap();
    let peer_id = anemo::PeerId(worker_name.0.to_bytes());
    network
        .connect_with_peer_id(address, peer_id)
        .await
        .unwrap();

    // AND a header
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(2))
        .build(author.keypair())
        .unwrap();
    let missing_digests = vec![BatchDigest::default()];
    let missing_digests_map = missing_digests
        .clone()
        .into_iter()
        .map(|d| (d, worker_id))
        .collect();

    // AND send a message to synchronizer batches
    tx_synchronizer
        .send(WaiterMessage::SyncBatches(
            missing_digests_map,
            header.clone(),
        ))
        .await
        .unwrap();

    // THEN
    if let Ok(Ok((_primary_msg, mut sync_msg))) =
        timeout(Duration::from_millis(4_000), handle).await
    {
        let msg = sync_msg.remove(0);
        assert_eq!(
            missing_digests, msg.digests,
            "Expected missing digests don't match"
        );

        // now simulate the write of the batch to the payload store
        payload_store
            .sync_write_all(missing_digests.into_iter().map(|e| ((e, worker_id), 1)))
            .await
            .unwrap();

        // now get the output as expected
        let header_result = rx_core.recv().await.unwrap();
        assert_eq!(header.digest(), header_result.digest());
    } else {
        panic!("Messages not received by worker");
    }
}
*/
