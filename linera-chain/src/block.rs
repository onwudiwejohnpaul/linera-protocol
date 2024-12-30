// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use async_graphql::SimpleObject;
use linera_base::{
    crypto::{BcsHashable, CryptoHash},
    data_types::{BlockHeight, OracleResponse, Timestamp},
    hashed::Hashed,
    identifiers::{ChainId, Owner},
};
use linera_execution::{committee::Epoch, Operation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    data_types::{EventRecord, ExecutedBlock, IncomingBundle, OutgoingMessage},
    ChainError,
};

/// Wrapper around an `ExecutedBlock` that has been validated.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct ValidatedBlock {
    executed_block: Hashed<ExecutedBlock>,
}

impl ValidatedBlock {
    /// Creates a new `ValidatedBlock` from an `ExecutedBlock`.
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self {
            executed_block: Hashed::new(executed_block),
        }
    }

    pub fn from_hashed(executed_block: Hashed<ExecutedBlock>) -> Self {
        Self { executed_block }
    }

    pub fn inner(&self) -> &Hashed<ExecutedBlock> {
        &self.executed_block
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ValidatedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.executed_block.inner()
    }

    /// Consumes this `ValidatedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block.into_inner()
    }

    pub fn to_log_str(&self) -> &'static str {
        "validated_block"
    }

    pub fn chain_id(&self) -> ChainId {
        self.executed_block().block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.executed_block().block.height
    }

    pub fn epoch(&self) -> Epoch {
        self.executed_block().block.epoch
    }
}

impl<'de> BcsHashable<'de> for ValidatedBlock {}

/// Wrapper around an `ExecutedBlock` that has been confirmed.
#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct ConfirmedBlock {
    // The executed block contained in this `ConfirmedBlock`.
    executed_block: Hashed<ExecutedBlock>,
}

#[async_graphql::Object(cache_control(no_cache))]
impl ConfirmedBlock {
    #[graphql(derived(name = "executed_block"))]
    async fn _executed_block(&self) -> ExecutedBlock {
        self.executed_block.inner().clone()
    }

    async fn status(&self) -> String {
        "confirmed".to_string()
    }
}

impl<'de> BcsHashable<'de> for ConfirmedBlock {}

impl ConfirmedBlock {
    pub fn new(executed_block: ExecutedBlock) -> Self {
        Self {
            executed_block: Hashed::new(executed_block),
        }
    }

    pub fn from_hashed(executed_block: Hashed<ExecutedBlock>) -> Self {
        Self { executed_block }
    }

    pub fn inner(&self) -> &Hashed<ExecutedBlock> {
        &self.executed_block
    }

    /// Returns a reference to the `ExecutedBlock` contained in this `ConfirmedBlock`.
    pub fn executed_block(&self) -> &ExecutedBlock {
        self.executed_block.inner()
    }

    /// Consumes this `ConfirmedBlock`, returning the `ExecutedBlock` it contains.
    pub fn into_inner(self) -> ExecutedBlock {
        self.executed_block.into_inner()
    }

    pub fn chain_id(&self) -> ChainId {
        self.executed_block.inner().block.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.executed_block.inner().block.height
    }

    pub fn to_log_str(&self) -> &'static str {
        "confirmed_block"
    }

    /// Creates a `HashedCertificateValue` without checking that this is the correct hash!
    pub fn with_hash_unchecked(self, hash: CryptoHash) -> Hashed<ConfirmedBlock> {
        Hashed::unchecked_new(self, hash)
    }

    fn with_hash(self) -> Hashed<Self> {
        let hash = CryptoHash::new(&self);
        Hashed::unchecked_new(self, hash)
    }

    /// Creates a `HashedCertificateValue` checking that this is the correct hash.
    pub fn with_hash_checked(self, hash: CryptoHash) -> Result<Hashed<ConfirmedBlock>, ChainError> {
        let hashed_certificate_value = self.with_hash();
        if hashed_certificate_value.hash() == hash {
            Ok(hashed_certificate_value)
        } else {
            Err(ChainError::CertificateValueHashMismatch {
                expected: hash,
                actual: hashed_certificate_value.hash(),
            })
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct Timeout {
    pub chain_id: ChainId,
    pub height: BlockHeight,
    pub epoch: Epoch,
}

impl Timeout {
    pub fn new(chain_id: ChainId, height: BlockHeight, epoch: Epoch) -> Self {
        Self {
            chain_id,
            height,
            epoch,
        }
    }

    pub fn to_log_str(&self) -> &'static str {
        "timeout"
    }

    pub fn chain_id(&self) -> ChainId {
        self.chain_id
    }

    pub fn height(&self) -> BlockHeight {
        self.height
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
}

impl<'de> BcsHashable<'de> for Timeout {}

/// Failure to convert a `Certificate` into one of the expected certificate types.
#[derive(Clone, Copy, Debug, Error)]
pub enum ConversionError {
    /// Failure to convert to [`ConfirmedBlock`] certificate.
    #[error("Expected a `ConfirmedBlockCertificate` value")]
    ConfirmedBlock,

    /// Failure to convert to [`ValidatedBlock`] certificate.
    #[error("Expected a `ValidatedBlockCertificate` value")]
    ValidatedBlock,

    /// Failure to convert to [`Timeout`] certificate.
    #[error("Expected a `TimeoutCertificate` value")]
    Timeout,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct Block {
    pub header: BlockHeader,
    pub body: BlockBody,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockHeader {
    pub version: u8, // TODO: More granular versioning.
    pub chain_id: ChainId,
    pub epoch: Epoch,
    pub height: BlockHeight,
    pub timestamp: Timestamp,
    pub state_hash: CryptoHash,
    pub previous_block_hash: Option<CryptoHash>,
    pub authenticated_signer: Option<Owner>,

    // Inputs to the block, chosen by the block proposer.
    pub bundles_hash: CryptoHash,
    pub operations_hash: CryptoHash,

    // Outcome of the block execution.
    pub messages_hash: CryptoHash,
    pub oracle_responses_hash: CryptoHash,
    pub events_hash: CryptoHash,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, SimpleObject)]
pub struct BlockBody {
    pub incoming_bundles: Vec<IncomingBundle>,
    pub operations: Vec<Operation>,
    pub messages: Vec<Vec<OutgoingMessage>>,
    pub oracle_responses: Vec<Vec<OracleResponse>>,
    pub events: Vec<Vec<EventRecord>>,
}

impl From<ExecutedBlock> for Block {
    fn from(executed_block: ExecutedBlock) -> Self {
        let bundles_hash = CryptoHash::new(&executed_block.block.incoming_bundles);
        let operations_hash = CryptoHash::new(&executed_block.block.operations);
        let messages_hash = CryptoHash::new(&executed_block.outcome.messages);
        let oracle_responses_hash = CryptoHash::new(&executed_block.outcome.oracle_responses);
        let events_hash = CryptoHash::new(&executed_block.outcome.events);

        Self {
            header: BlockHeader {
                version: 1,
                chain_id: executed_block.block.chain_id,
                epoch: executed_block.block.epoch,
                height: executed_block.block.height,
                timestamp: executed_block.block.timestamp,
                state_hash: executed_block.outcome.state_hash,
                previous_block_hash: executed_block.block.previous_block_hash,
                authenticated_signer: executed_block.block.authenticated_signer,
                bundles_hash,
                operations_hash,
                messages_hash,
                oracle_responses_hash,
                events_hash,
            },
            body: BlockBody {
                incoming_bundles: executed_block.block.incoming_bundles,
                operations: executed_block.block.operations,
                messages: executed_block.outcome.messages,
                oracle_responses: executed_block.outcome.oracle_responses,
                events: executed_block.outcome.events,
            },
        }
    }
}

// TODO: Implement Hashable<Hasher> for Block that hashes its header and body
// as a merkle trie.
impl<'de> BcsHashable<'de> for Block {}
