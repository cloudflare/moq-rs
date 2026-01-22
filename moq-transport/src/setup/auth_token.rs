//! Authorization Token support for MOQT.
//!
//! This module provides support for authorization tokens as defined in the MOQT specification.
//! Tokens can be sent inline or referenced by alias to avoid retransmission of large tokens.

use std::collections::HashMap;

/// Authorization Token Types
///
/// Defines how an authorization token is transmitted in messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum AuthTokenType {
    /// No authorization token present
    None = 0x0,
    /// Authorization token sent inline
    Inline = 0x1,
    /// Authorization token referenced by alias
    Alias = 0x2,
    /// Authorization token cached with new alias
    Store = 0x3,
    /// Use previously stored token (DELETE is not allowed in CLIENT_SETUP)
    UseAlias = 0x4,
}

impl TryFrom<u8> for AuthTokenType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x0 => Ok(Self::None),
            0x1 => Ok(Self::Inline),
            0x2 => Ok(Self::Alias),
            0x3 => Ok(Self::Store),
            0x4 => Ok(Self::UseAlias),
            _ => Err(()),
        }
    }
}

/// An authorization token value
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuthToken {
    /// The raw token bytes
    pub token: Vec<u8>,
    /// Optional alias for caching
    pub alias: Option<u64>,
}

impl AuthToken {
    /// Create a new authorization token
    pub fn new(token: Vec<u8>) -> Self {
        Self { token, alias: None }
    }

    /// Create a new authorization token with an alias for caching
    pub fn with_alias(token: Vec<u8>, alias: u64) -> Self {
        Self {
            token,
            alias: Some(alias),
        }
    }

    /// Check if the token is empty
    pub fn is_empty(&self) -> bool {
        self.token.is_empty()
    }
}

/// Authorization Token Cache
///
/// Stores authorization tokens by their alias for efficient re-use across multiple messages.
/// The cache enforces a maximum size limit as negotiated during setup.
#[derive(Debug)]
pub struct AuthTokenCache {
    /// Maximum number of tokens that can be cached
    max_size: usize,
    /// Cached tokens by alias
    tokens: HashMap<u64, Vec<u8>>,
    /// Next available alias (for server-assigned aliases)
    next_alias: u64,
}

impl Default for AuthTokenCache {
    fn default() -> Self {
        Self::new(0)
    }
}

impl AuthTokenCache {
    /// Create a new auth token cache with the specified maximum size
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            tokens: HashMap::new(),
            next_alias: 0,
        }
    }

    /// Get the maximum cache size
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Set the maximum cache size (typically from setup negotiation)
    pub fn set_max_size(&mut self, max_size: usize) {
        self.max_size = max_size;
    }

    /// Get the current number of cached tokens
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Check if the cache is at capacity
    pub fn is_full(&self) -> bool {
        self.tokens.len() >= self.max_size
    }

    /// Store a token with the given alias
    ///
    /// Returns an error if:
    /// - The cache is at capacity
    /// - The alias is already in use
    pub fn store(&mut self, alias: u64, token: Vec<u8>) -> Result<(), AuthTokenCacheError> {
        if self.max_size == 0 {
            return Err(AuthTokenCacheError::CacheDisabled);
        }
        if self.tokens.len() >= self.max_size {
            return Err(AuthTokenCacheError::CacheOverflow);
        }
        if self.tokens.contains_key(&alias) {
            return Err(AuthTokenCacheError::DuplicateAlias(alias));
        }
        self.tokens.insert(alias, token);
        Ok(())
    }

    /// Store a token with an auto-generated alias
    ///
    /// Returns the assigned alias, or an error if the cache is full
    pub fn store_with_auto_alias(&mut self, token: Vec<u8>) -> Result<u64, AuthTokenCacheError> {
        if self.max_size == 0 {
            return Err(AuthTokenCacheError::CacheDisabled);
        }
        if self.tokens.len() >= self.max_size {
            return Err(AuthTokenCacheError::CacheOverflow);
        }

        // Find next available alias
        while self.tokens.contains_key(&self.next_alias) {
            self.next_alias = self.next_alias.wrapping_add(1);
        }

        let alias = self.next_alias;
        self.tokens.insert(alias, token);
        self.next_alias = self.next_alias.wrapping_add(1);

        Ok(alias)
    }

    /// Get a token by its alias
    pub fn get(&self, alias: u64) -> Option<&Vec<u8>> {
        self.tokens.get(&alias)
    }

    /// Remove a token by its alias
    pub fn remove(&mut self, alias: u64) -> Option<Vec<u8>> {
        self.tokens.remove(&alias)
    }

    /// Clear all cached tokens
    pub fn clear(&mut self) {
        self.tokens.clear();
    }
}

/// Errors that can occur when working with the auth token cache
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthTokenCacheError {
    /// The cache is disabled (max_size is 0)
    CacheDisabled,
    /// The cache is full and cannot accept more tokens
    CacheOverflow,
    /// The alias is already in use
    DuplicateAlias(u64),
    /// The alias was not found in the cache
    UnknownAlias(u64),
    /// The token is malformed
    MalformedToken,
    /// The token has expired
    ExpiredToken,
}

impl std::fmt::Display for AuthTokenCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CacheDisabled => write!(f, "authorization token cache is disabled"),
            Self::CacheOverflow => write!(f, "authorization token cache is full"),
            Self::DuplicateAlias(alias) => {
                write!(f, "duplicate authorization token alias: {}", alias)
            }
            Self::UnknownAlias(alias) => {
                write!(f, "unknown authorization token alias: {}", alias)
            }
            Self::MalformedToken => write!(f, "malformed authorization token"),
            Self::ExpiredToken => write!(f, "expired authorization token"),
        }
    }
}

impl std::error::Error for AuthTokenCacheError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_token_type_conversion() {
        assert_eq!(AuthTokenType::try_from(0u8), Ok(AuthTokenType::None));
        assert_eq!(AuthTokenType::try_from(1u8), Ok(AuthTokenType::Inline));
        assert_eq!(AuthTokenType::try_from(2u8), Ok(AuthTokenType::Alias));
        assert_eq!(AuthTokenType::try_from(3u8), Ok(AuthTokenType::Store));
        assert_eq!(AuthTokenType::try_from(4u8), Ok(AuthTokenType::UseAlias));
        assert!(AuthTokenType::try_from(5u8).is_err());
    }

    #[test]
    fn test_auth_token() {
        let token = AuthToken::new(vec![1, 2, 3, 4]);
        assert!(!token.is_empty());
        assert!(token.alias.is_none());

        let token_with_alias = AuthToken::with_alias(vec![5, 6, 7, 8], 42);
        assert_eq!(token_with_alias.alias, Some(42));

        let empty_token = AuthToken::default();
        assert!(empty_token.is_empty());
    }

    #[test]
    fn test_auth_token_cache() {
        let mut cache = AuthTokenCache::new(3);
        assert_eq!(cache.max_size(), 3);
        assert!(cache.is_empty());

        // Store tokens
        cache.store(1, vec![1, 2, 3]).unwrap();
        cache.store(2, vec![4, 5, 6]).unwrap();
        assert_eq!(cache.len(), 2);
        assert!(!cache.is_full());

        // Get token
        assert_eq!(cache.get(1), Some(&vec![1, 2, 3]));
        assert_eq!(cache.get(2), Some(&vec![4, 5, 6]));
        assert_eq!(cache.get(3), None);

        // Store with auto-alias
        let alias = cache.store_with_auto_alias(vec![7, 8, 9]).unwrap();
        assert!(cache.is_full());

        // Cache overflow
        assert_eq!(
            cache.store(99, vec![10, 11]),
            Err(AuthTokenCacheError::CacheOverflow)
        );

        // Duplicate alias
        cache.remove(alias);
        assert_eq!(
            cache.store(1, vec![10, 11]),
            Err(AuthTokenCacheError::DuplicateAlias(1))
        );

        // Remove and clear
        assert!(cache.remove(1).is_some());
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_auth_token_cache_disabled() {
        let mut cache = AuthTokenCache::new(0);
        assert_eq!(
            cache.store(1, vec![1, 2, 3]),
            Err(AuthTokenCacheError::CacheDisabled)
        );
        assert_eq!(
            cache.store_with_auto_alias(vec![1, 2, 3]),
            Err(AuthTokenCacheError::CacheDisabled)
        );
    }
}
