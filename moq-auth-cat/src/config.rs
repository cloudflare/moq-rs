// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc. and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::sync::Arc;

use cat_token::{CatTokenValidator, CryptographicAlgorithm, MoqtValidator};

/// Configuration for building a C4MAuthHook.
pub struct C4MConfig {
    pub(crate) algorithm: Arc<dyn CryptographicAlgorithm + Send + Sync>,
    pub(crate) token_validator: CatTokenValidator,
    pub(crate) moqt_validator: MoqtValidator,
}

impl C4MConfig {
    pub fn new(algorithm: impl CryptographicAlgorithm + Send + Sync + 'static) -> Self {
        Self {
            algorithm: Arc::new(algorithm),
            token_validator: CatTokenValidator::new(),
            moqt_validator: MoqtValidator::new(),
        }
    }

    pub fn with_expected_issuers(mut self, issuers: Vec<String>) -> Self {
        self.token_validator = self.token_validator.with_expected_issuers(issuers);
        self
    }

    pub fn with_expected_audiences(mut self, audiences: Vec<String>) -> Self {
        self.token_validator = self.token_validator.with_expected_audiences(audiences);
        self
    }

    pub fn with_clock_skew_tolerance(mut self, seconds: i64) -> Self {
        self.token_validator = self.token_validator.with_clock_skew_tolerance(seconds);
        self
    }

    pub fn with_min_revalidation_interval(mut self, seconds: f64) -> Self {
        self.moqt_validator = self.moqt_validator.with_min_revalidation_interval(seconds);
        self
    }
}
