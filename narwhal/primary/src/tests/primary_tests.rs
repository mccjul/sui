// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::{NetworkModel, Primary, PrimaryReceiverHandler, CHANNEL_CAPACITY};
use crate::{common::create_db_stores, metrics::{PrimaryChannelMetrics, PrimaryMetrics}, PayloadToken, synchronizer::Synchronizer};
use arc_swap::ArcSwap;
use bincode::Options;
use config::{Parameters, WorkerId};
use consensus::{dag::Dag, metrics::ConsensusMetrics};
use crypto::PublicKey;
use fastcrypto::{hash::Hash, traits::KeyPair, SignatureService};
use itertools::Itertools;
use node::NodeStorage;
use prometheus::Registry;
use std::{
    borrow::Borrow,
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use storage::{CertificateStore, ProposerStore};
use store::rocks::DBMap;
use store::Store;
use test_utils::{temp_dir, CommitteeFixture};
use tokio::sync::{watch, Mutex};
use types::{
    BatchDigest, Certificate, CertificateDigest, FetchCertificatesRequest,
    PayloadAvailabilityRequest, PrimaryToPrimary, ReconfigureNotification, Round,
};
use worker::{metrics::initialise_metrics, Worker};

#[tokio::test]
async fn get_network_peers_from_admin_server() {
    // telemetry_subscribers::init_for_testing();
    let primary_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let authority_1 = fixture.authorities().next().unwrap();
    let name_1 = authority_1.public_key();
    let signer_1 = authority_1.keypair().copy();

    let worker_id = 0;
    let worker_1_keypair = authority_1.worker(worker_id).keypair().copy();

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir());

    let (tx_new_certificates, rx_new_certificates) = types::metered_channel::channel(
        CHANNEL_CAPACITY,
        &prometheus::IntGauge::new(
            PrimaryChannelMetrics::NAME_NEW_CERTS,
            PrimaryChannelMetrics::DESC_NEW_CERTS,
        )
        .unwrap(),
    );
    let (tx_feedback, rx_feedback) = types::metered_channel::channel(
        CHANNEL_CAPACITY,
        &prometheus::IntGauge::new(
            PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
            PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
        )
        .unwrap(),
    );
    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (tx_reconfigure, _rx_reconfigure) = watch::channel(initial_committee);
    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    // Spawn Primary 1
    Primary::spawn(
        name_1.clone(),
        signer_1,
        authority_1.network_keypair().copy(),
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        primary_1_parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.proposer_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
        /* dag */
        Some(Arc::new(
            Dag::new(&committee, rx_new_certificates, consensus_metrics).1,
        )),
        NetworkModel::Asynchronous,
        tx_reconfigure,
        tx_feedback,
        &Registry::new(),
        None,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_1 = Registry::new();
    let metrics_1 = initialise_metrics(&registry_1);

    let worker_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Spawn a `Worker` instance for primary 1.
    Worker::spawn(
        name_1,
        worker_1_keypair.copy(),
        worker_id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        worker_1_parameters.clone(),
        store.batch_store,
        metrics_1,
    );

    // Test getting all known peers for primary 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        primary_1_parameters
            .network_admin_server
            .primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 19 peers (3 other primaries + 4 workers + 4*3 other workers)
    assert_eq!(19, resp.len());

    // Test getting all connected peers for primary 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_1_parameters
            .network_admin_server
            .primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 1 peers (only 1 worker spawned)
    assert_eq!(1, resp.len());

    let authority_2 = fixture.authorities().nth(1).unwrap();
    let name_2 = authority_2.public_key();
    let signer_2 = authority_2.keypair().copy();

    let primary_2_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // TODO: Rework test-utils so that macro can be used for the channels below.
    let (tx_new_certificates_2, rx_new_certificates_2) = types::metered_channel::channel(
        CHANNEL_CAPACITY,
        &prometheus::IntGauge::new(
            PrimaryChannelMetrics::NAME_NEW_CERTS,
            PrimaryChannelMetrics::DESC_NEW_CERTS,
        )
        .unwrap(),
    );
    let (tx_feedback_2, rx_feedback_2) = types::metered_channel::channel(
        CHANNEL_CAPACITY,
        &prometheus::IntGauge::new(
            PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
            PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
        )
        .unwrap(),
    );
    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (tx_reconfigure_2, _rx_reconfigure_2) = watch::channel(initial_committee);
    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    // Spawn Primary 2
    Primary::spawn(
        name_2.clone(),
        signer_2,
        authority_2.network_keypair().copy(),
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        primary_2_parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.proposer_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        /* tx_consensus */ tx_new_certificates_2,
        /* rx_consensus */ rx_feedback_2,
        /* dag */
        Some(Arc::new(
            Dag::new(&committee, rx_new_certificates_2, consensus_metrics).1,
        )),
        NetworkModel::Asynchronous,
        tx_reconfigure_2,
        tx_feedback_2,
        &Registry::new(),
        None,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let primary_1_peer_id = hex::encode(authority_1.network_keypair().copy().public().0.as_bytes());
    let primary_2_peer_id = hex::encode(authority_2.network_keypair().copy().public().0.as_bytes());
    let worker_1_peer_id = hex::encode(worker_1_keypair.copy().public().0.as_bytes());

    // Test getting all connected peers for primary 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_1_parameters
            .network_admin_server
            .primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 other primary spawned + 1 worker spawned)
    assert_eq!(2, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = vec![&primary_2_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));

    // Test getting all connected peers for primary 2
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        primary_2_parameters
            .network_admin_server
            .primary_network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 other primary spawned + 1 other worker)
    assert_eq!(2, resp.len());

    // Assert peer ids are correct
    let expected_peer_ids = vec![&primary_1_peer_id, &worker_1_peer_id];
    assert!(expected_peer_ids.iter().all(|e| resp.contains(e)));
}

/*
// TODO-DNS moved from Core
#[tokio::test]
async fn process_header() {
    telemetry_subscribers::init_for_testing();

    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let author = fixture.authorities().next().unwrap();
    let primary = fixture.authorities().nth(1).unwrap();

    let header = author.header(&committee);

    let network_key = primary.network_keypair().copy().private().0.to_bytes();
    let name = primary.public_key();
    let mut signature_service = SignatureService::new(primary.keypair().copy());

    let (_tx_reconfigure, rx_reconfigure) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_sync_headers, _rx_sync_headers) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (tx_primary_messages, rx_primary_messages) = test_utils::test_channel!(1);
    let (_tx_headers_loopback, rx_headers_loopback) = test_utils::test_channel!(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = test_utils::test_channel!(1);
    let (_tx_headers, rx_headers) = test_utils::test_channel!(1);
    let (tx_consensus, _rx_consensus) = test_utils::test_channel!(1);
    let (tx_parents, _rx_parents) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    // Create test stores.
    let (header_store, certificates_store, payload_store) = create_db_stores();

    // Make the vote we expect to receive.
    let expected = Vote::new(&header, &name, &mut signature_service).await;

    // Spawn a listener to receive the vote.
    let address = committee.primary(&header.author).unwrap();
    let (mut handle, _network) =
        PrimaryToPrimaryMockServer::spawn(author.network_keypair().copy(), address.clone());

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name.clone(),
        &committee,
        certificates_store.clone(),
        payload_store,
        /* tx_header_waiter */ tx_sync_headers,
        tx_certificate_waiter,
        None,
    );

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let own_address = network::multiaddr_to_address(&committee.primary(&name).unwrap()).unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();

    let address = network::multiaddr_to_address(&address).unwrap();
    let network_key = author.network_keypair().public().0.to_bytes();
    let peer_info = PeerInfo {
        peer_id: PeerId(network_key),
        affinity: anemo::types::PeerAffinity::High,
        address: vec![address],
    };
    network.known_peers().insert(peer_info);

    // Spawn the core.
    let _core_handle = Core::spawn(
        name.clone(),
        committee.clone(),
        worker_cache,
        header_store.clone(),
        certificates_store.clone(),
        create_test_vote_store(),
        synchronizer,
        signature_service,
        rx_consensus_round_updates,
        /* gc_depth */ 50,
        rx_reconfigure,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        metrics.clone(),
        P2pNetwork::new(network),
    );

    // Send a header to the core.
    tx_primary_messages
        .send(PrimaryMessage::Header(header.clone()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote.
    match handle.recv().await.unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    // Ensure the header is correctly stored.
    let stored = header_store.read(header.digest()).await.unwrap();
    assert_eq!(stored, Some(header.clone()));

    let mut m = HashMap::new();
    m.insert("epoch", "0");
    m.insert("source", "other");
    assert_eq!(
        metrics.headers_processed.get_metric_with(&m).unwrap().get(),
        1
    );

    // Test idempotence by re-sending the same header and expecting the vote

    // Send the header to the core again.
    tx_primary_messages
        .send(PrimaryMessage::Header(header.clone()))
        .await
        .unwrap();

    // Ensure the listener correctly received the vote again.
    match handle.recv().await.unwrap() {
        PrimaryMessage::Vote(x) => assert_eq!(x, expected),
        x => panic!("Unexpected message: {:?}", x),
    }

    assert_eq!(
        metrics.headers_processed.get_metric_with(&m).unwrap().get(),
        2
    );
}

#[tokio::test]
async fn process_header_missing_parent() {
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let network_key = primary.network_keypair().copy().private().0.to_bytes();
    let name = primary.public_key();
    let signature_service = SignatureService::new(primary.keypair().copy());

    let (_, rx_reconfigure) = watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_sync_headers, _rx_sync_headers) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (tx_primary_messages, rx_primary_messages) = test_utils::test_channel!(1);
    let (_tx_headers_loopback, rx_headers_loopback) = test_utils::test_channel!(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = test_utils::test_channel!(1);
    let (_tx_headers, rx_headers) = test_utils::test_channel!(1);
    let (tx_consensus, _rx_consensus) = test_utils::test_channel!(1);
    let (tx_parents, _rx_parents) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    // Create test stores.
    let (header_store, certificates_store, payload_store) = create_db_stores();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name.clone(),
        &committee,
        certificates_store.clone(),
        payload_store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        tx_certificate_waiter,
        None,
    );

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let own_address = network::multiaddr_to_address(&committee.primary(&name).unwrap()).unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();

    // Spawn the core.
    let _core_handle = Core::spawn(
        name.clone(),
        committee.clone(),
        worker_cache,
        header_store.clone(),
        certificates_store.clone(),
        create_test_vote_store(),
        synchronizer,
        signature_service,
        rx_consensus_round_updates,
        /* gc_depth */ 50,
        rx_reconfigure,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        metrics.clone(),
        P2pNetwork::new(network),
    );

    // Send a header to the core.
    let builder = types::HeaderBuilder::default();
    let header = builder
        .author(name.clone())
        .round(1)
        .epoch(0)
        .parents([CertificateDigest::default()].iter().cloned().collect())
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build(primary.keypair())
        .unwrap();

    let digest = header.digest();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(header_store.read(digest).await.unwrap().is_none());
}

#[tokio::test]
async fn process_header_missing_payload() {
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let network_key = primary.network_keypair().copy().private().0.to_bytes();
    let name = primary.public_key();
    let signature_service = SignatureService::new(primary.keypair().copy());

    let (_, rx_reconfigure) = watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_sync_headers, _rx_sync_headers) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (tx_primary_messages, rx_primary_messages) = test_utils::test_channel!(1);
    let (_tx_headers_loopback, rx_headers_loopback) = test_utils::test_channel!(1);
    let (_tx_certificates_loopback, rx_certificates_loopback) = test_utils::test_channel!(1);
    let (_tx_headers, rx_headers) = test_utils::test_channel!(1);
    let (tx_consensus, _rx_consensus) = test_utils::test_channel!(1);
    let (tx_parents, _rx_parents) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    // Create test stores.
    let (header_store, certificates_store, payload_store) = create_db_stores();

    // Make a synchronizer for the core.
    let synchronizer = Synchronizer::new(
        name.clone(),
        &committee,
        certificates_store.clone(),
        payload_store.clone(),
        /* tx_header_waiter */ tx_sync_headers,
        tx_certificate_waiter,
        None,
    );

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let own_address = network::multiaddr_to_address(&committee.primary(&name).unwrap()).unwrap();
    let network = anemo::Network::bind(own_address)
        .server_name("narwhal")
        .private_key(network_key)
        .start(anemo::Router::new())
        .unwrap();

    // Spawn the core.
    let _core_handle = Core::spawn(
        name.clone(),
        committee.clone(),
        worker_cache,
        header_store.clone(),
        certificates_store.clone(),
        create_test_vote_store(),
        synchronizer,
        signature_service,
        rx_consensus_round_updates,
        /* gc_depth */ 50,
        rx_reconfigure,
        /* rx_primaries */ rx_primary_messages,
        /* rx_header_waiter */ rx_headers_loopback,
        /* rx_certificate_waiter */ rx_certificates_loopback,
        /* rx_proposer */ rx_headers,
        tx_consensus,
        /* tx_proposer */ tx_parents,
        metrics.clone(),
        P2pNetwork::new(network),
    );

    // Send a header that another node has created to the core.
    // We need this header to be another's node, because our own
    // created headers are not checked against having a payload.
    // Just take another keys other than this node's.
    let author = fixture.authorities().nth(1).unwrap();
    let header = author
        .header_builder(&committee)
        .with_payload_batch(fixture_batch_with_transactions(10), 0)
        .build(author.keypair())
        .unwrap();

    let digest = header.digest();
    tx_primary_messages
        .send(PrimaryMessage::Header(header))
        .await
        .unwrap();

    // Ensure the header is not stored.
    assert!(header_store.read(digest).await.unwrap().is_none());
}
*/

#[tokio::test]
async fn test_fetch_certificates_handler() {
    let fixture = CommitteeFixture::builder()
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let name = fixture.authorities().next().unwrap().public_key();
    let worker_cache = fixture.shared_worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let signature_service = SignatureService::new(primary.keypair().copy());
    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let (header_store, certificate_store, payload_store) = create_db_stores();
    let (tx_certificates, _) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let synchronizer = Arc::new(Synchronizer::new(
        name.clone(),
        fixture.committee().into(),
        worker_cache.clone(),
        certificate_store.clone(),
        payload_store.clone(),
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        None,
    ));
    let handler = PrimaryReceiverHandler {
        name,
        committee: fixture.committee().into(),
        worker_cache: worker_cache.clone(),
        synchronizer: synchronizer.clone(),
        signature_service,
        tx_certificates,
        header_store: header_store.clone(),
        certificate_store: certificate_store.clone(),
        payload_store: payload_store.clone(),
        proposer_store: ProposerStore::new_for_tests(),
        vote_digest_store: crate::common::create_test_vote_store(),
        rx_consensus_round_updates: rx_consensus_round_updates.clone(),
        metrics: metrics.clone(),
        request_vote_inflight: Arc::new(Mutex::new(HashSet::new())),
    };

    let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
        .into_iter()
        .map(|cert| cert.header)
        .collect();
    let mut headers = vec![];
    let total_rounds = 4;
    for i in 0..total_rounds {
        let parents: BTreeSet<_> = current_round
            .into_iter()
            .map(|header| fixture.certificate(&header).digest())
            .collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }

    let total_authorities = fixture.authorities().count();
    let total_certificates = total_authorities * total_rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates);
    assert_eq!(16, total_certificates);

    // Populate certificate store such that each authority has the following rounds:
    // Authority 0: 1
    // Authority 1: 1 2
    // Authority 2: 1 2 3
    // Authority 3: 1 2 3 4
    // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
    // already in store. But this does not matter for testing here.
    let mut authorities = Vec::<PublicKey>::new();
    for i in 0..total_authorities {
        authorities.push(certificates[i].header.author.clone());
        for j in 0..=i {
            let cert = certificates[i + j * total_authorities].clone();
            assert_eq!(&cert.header.author, authorities.last().unwrap());
            certificate_store
                .write(cert)
                .expect("Writing certificate to store failed");
        }
    }

    // Each test case contains (lower bound round, skip rounds, max items, expected output).
    let test_cases = vec![
        (
            0,
            vec![vec![], vec![], vec![], vec![]],
            20,
            vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4],
        ),
        (
            0,
            vec![vec![1u64], vec![1], vec![], vec![]],
            20,
            vec![1, 1, 2, 2, 2, 3, 3, 4],
        ),
        (
            0,
            vec![vec![], vec![], vec![1], vec![1]],
            20,
            vec![1, 1, 2, 2, 2, 3, 3, 4],
        ),
        (
            1,
            vec![vec![], vec![], vec![2], vec![2]],
            4,
            vec![2, 3, 3, 4],
        ),
        (1, vec![vec![], vec![], vec![2], vec![2]], 2, vec![2, 3]),
        (
            0,
            vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]],
            2,
            vec![2, 4],
        ),
        (2, vec![vec![], vec![], vec![], vec![]], 3, vec![3, 3, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], 2, vec![3, 3]),
        // Check that round 2 and 4 are fetched for the last authority, skipping round 3.
        (
            1,
            vec![vec![], vec![], vec![3], vec![3]],
            5,
            vec![2, 2, 2, 4],
        ),
    ];
    for (lower_bound_round, skip_rounds_vec, max_items, expected_rounds) in test_cases {
        let req = FetchCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(
                        skip_rounds_vec
                            .into_iter()
                            .map(|rounds| rounds.into_iter().collect()),
                    )
                    .collect(),
            )
            .set_max_items(max_items);
        let resp = handler
            .fetch_certificates(anemo::Request::new(req.clone()))
            .await
            .unwrap()
            .into_body();
        assert_eq!(
            resp.certificates
                .iter()
                .map(|cert| cert.round())
                .collect_vec(),
            expected_rounds
        );
    }
}

#[tokio::test]
async fn test_process_payload_availability_success() {
    let fixture = CommitteeFixture::builder()
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let author = fixture.authorities().next().unwrap();
    let name = author.public_key();
    let worker_cache = fixture.shared_worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let signature_service = SignatureService::new(primary.keypair().copy());
    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let (header_store, certificate_store, payload_store) = create_db_stores();
    let (tx_certificates, _) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let synchronizer = Arc::new(Synchronizer::new(
        name.clone(),
        fixture.committee().into(),
        worker_cache.clone(),
        certificate_store.clone(),
        payload_store.clone(),
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        None,
    ));
    let handler = PrimaryReceiverHandler {
        name,
        committee: fixture.committee().into(),
        worker_cache: worker_cache.clone(),
        synchronizer: synchronizer.clone(),
        signature_service,
        tx_certificates,
        header_store: header_store.clone(),
        certificate_store: certificate_store.clone(),
        payload_store: payload_store.clone(),
        proposer_store: ProposerStore::new_for_tests(),
        vote_digest_store: crate::common::create_test_vote_store(),
        rx_consensus_round_updates: rx_consensus_round_updates.clone(),
        metrics: metrics.clone(),
        request_vote_inflight: Arc::new(Mutex::new(HashSet::new())),
    };

    // GIVEN some mock certificates
    let mut certificates = HashMap::new();
    let mut missing_certificates = HashSet::new();

    for i in 0..10 {
        let header = author
            .header_builder(&fixture.committee())
            .with_payload_batch(test_utils::fixture_batch_with_transactions(10), 0)
            .build(author.keypair())
            .unwrap();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());

        // We want to simulate the scenario of both having some certificates
        // found and some non found. Store only the half. The other half
        // should be returned back as non found.
        if i < 7 {
            // write the certificate
            certificate_store.write(certificate.clone()).unwrap();

            for payload in certificate.header.payload {
                payload_store.async_write(payload, 1).await;
            }
        } else {
            missing_certificates.insert(digest);
        }
    }

    // WHEN requesting the payload availability for all the certificates
    let request = anemo::Request::new(PayloadAvailabilityRequest {
        certificate_digests: certificates.keys().copied().collect(),
    });
    let response = handler.get_payload_availability(request).await.unwrap();
    let result_digests: HashSet<CertificateDigest> = response
        .body()
        .payload_availability
        .iter()
        .map(|(digest, _)| *digest)
        .collect();

    assert_eq!(
        result_digests.len(),
        certificates.len(),
        "Returned unique number of certificates don't match the expected"
    );

    // ensure that we have no payload availability for some
    let availability_map = response
        .into_body()
        .payload_availability
        .into_iter()
        .counts_by(|c| c.1);

    for (available, found) in availability_map {
        if available {
            assert_eq!(found, 7, "Expected to have available payloads");
        } else {
            assert_eq!(found, 3, "Expected to have non available payloads");
        }
    }
}

#[tokio::test]
async fn test_process_payload_availability_when_failures() {
    // GIVEN
    // We initialise the test stores manually to allow us
    // inject some wrongly serialised values to cause data store errors.
    let rocksdb = store::rocks::open_cf(
        temp_dir(),
        None,
        &[
            test_utils::CERTIFICATES_CF,
            test_utils::CERTIFICATE_DIGEST_BY_ROUND_CF,
            test_utils::CERTIFICATE_DIGEST_BY_ORIGIN_CF,
            test_utils::PAYLOAD_CF,
        ],
    )
    .expect("Failed creating database");

    let (
        certificate_map,
        certificate_digest_by_round_map,
        certificate_digest_by_origin_map,
        payload_map,
    ) = store::reopen!(&rocksdb,
        test_utils::CERTIFICATES_CF;<CertificateDigest, Certificate>,
        test_utils::CERTIFICATE_DIGEST_BY_ROUND_CF;<(Round, PublicKey), CertificateDigest>,
        test_utils::CERTIFICATE_DIGEST_BY_ORIGIN_CF;<(PublicKey, Round), CertificateDigest>,
        test_utils::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>);

    let certificate_store = CertificateStore::new(
        certificate_map,
        certificate_digest_by_round_map,
        certificate_digest_by_origin_map,
    );
    let payload_store: Store<(BatchDigest, WorkerId), PayloadToken> = Store::new(payload_map);

    let fixture = CommitteeFixture::builder()
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let committee = fixture.committee();
    let author = fixture.authorities().next().unwrap();
    let name = author.public_key();
    let worker_cache = fixture.shared_worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let signature_service = SignatureService::new(primary.keypair().copy());
    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    let (header_store, _, _) = create_db_stores();
    let (tx_certificates, _) = test_utils::test_channel!(1);
    let (tx_certificate_waiter, _rx_certificate_waiter) = test_utils::test_channel!(1);
    let (_tx_consensus_round_updates, rx_consensus_round_updates) = watch::channel(0u64);

    let synchronizer = Arc::new(Synchronizer::new(
        name.clone(),
        fixture.committee().into(),
        worker_cache.clone(),
        certificate_store.clone(),
        payload_store.clone(),
        tx_certificate_waiter,
        rx_consensus_round_updates.clone(),
        None,
    ));
    let handler = PrimaryReceiverHandler {
        name,
        committee: fixture.committee().into(),
        worker_cache: worker_cache.clone(),
        synchronizer: synchronizer.clone(),
        signature_service,
        tx_certificates,
        header_store: header_store.clone(),
        certificate_store: certificate_store.clone(),
        payload_store: payload_store.clone(),
        proposer_store: ProposerStore::new_for_tests(),
        vote_digest_store: crate::common::create_test_vote_store(),
        rx_consensus_round_updates: rx_consensus_round_updates.clone(),
        metrics: metrics.clone(),
        request_vote_inflight: Arc::new(Mutex::new(HashSet::new())),
    };

    // AND some mock certificates
    let mut certificate_digests = Vec::new();
    for _ in 0..10 {
        let header = author
            .header_builder(&committee)
            .with_payload_batch(test_utils::fixture_batch_with_transactions(10), 0)
            .build(author.keypair())
            .unwrap();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        // In order to test an error scenario that is coming from the data store,
        // we are going to store for the provided certificate digests some unexpected
        // payload in order to blow up the deserialisation.
        let serialised_key = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding()
            .serialize(&digest.borrow())
            .expect("Couldn't serialise key");

        // Just serialise the "false" value
        let dummy_value = bincode::serialize(false.borrow()).expect("Couldn't serialise value");

        rocksdb
            .put_cf(
                &rocksdb
                    .cf_handle(test_utils::CERTIFICATES_CF)
                    .expect("Couldn't find column family"),
                serialised_key,
                dummy_value,
            )
            .expect("Couldn't insert value");

        certificate_digests.push(digest);
    }

    // WHEN requesting the payload availability for all the certificates
    let request = anemo::Request::new(PayloadAvailabilityRequest {
        certificate_digests,
    });
    let result = handler.get_payload_availability(request).await;
    assert!(result.is_err(), "expected error reading certificates");
}
