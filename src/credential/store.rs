use aes_gcm::{Aes256Gcm, KeyInit, Nonce, aead::Aead};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;
use serde::{Deserialize, Serialize};

use oversync_core::error::OversyncError;

/// Type of credential stored.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CredentialType {
	Password,
	Token,
	Keypair,
}

impl std::fmt::Display for CredentialType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Password => write!(f, "password"),
			Self::Token => write!(f, "token"),
			Self::Keypair => write!(f, "keypair"),
		}
	}
}

/// A credential with a plaintext secret (in memory only, encrypted at rest).
#[derive(Debug, Clone)]
pub struct Credential {
	pub name: String,
	pub credential_type: CredentialType,
	pub secret: String,
}

/// AES-256-GCM encryption/decryption for credential secrets.
///
/// The 32-byte key is derived from a passphrase via SHA-256.
/// Each encryption uses a random 12-byte nonce, prepended to the ciphertext.
pub struct AesGcmStore {
	cipher: Aes256Gcm,
}

impl AesGcmStore {
	/// Create a store from a passphrase. The passphrase is hashed to derive a 256-bit key.
	pub fn from_passphrase(passphrase: &str) -> Self {
		use sha2::{Digest, Sha256};
		let key = Sha256::digest(passphrase.as_bytes());
		let cipher = Aes256Gcm::new_from_slice(&key).expect("SHA-256 always produces 32 bytes");
		Self { cipher }
	}

	/// Create a store from a raw 32-byte key.
	pub fn from_key(key: &[u8; 32]) -> Self {
		let cipher = Aes256Gcm::new_from_slice(key).expect("32-byte key is valid for AES-256");
		Self { cipher }
	}

	/// Encrypt a plaintext secret. Returns base64(nonce || ciphertext).
	pub fn encrypt(&self, plaintext: &str) -> Result<String, OversyncError> {
		let mut nonce_bytes = [0u8; 12];
		rand::rng().fill_bytes(&mut nonce_bytes);
		let nonce = Nonce::from_slice(&nonce_bytes);

		let ciphertext = self
			.cipher
			.encrypt(nonce, plaintext.as_bytes())
			.map_err(|e| OversyncError::Internal(format!("encrypt: {e}")))?;

		let mut combined = Vec::with_capacity(12 + ciphertext.len());
		combined.extend_from_slice(&nonce_bytes);
		combined.extend_from_slice(&ciphertext);

		Ok(BASE64.encode(&combined))
	}

	/// Decrypt a base64(nonce || ciphertext) string back to plaintext.
	pub fn decrypt(&self, encoded: &str) -> Result<String, OversyncError> {
		let combined = BASE64
			.decode(encoded)
			.map_err(|e| OversyncError::Internal(format!("base64 decode: {e}")))?;

		if combined.len() < 13 {
			return Err(OversyncError::Internal(
				"ciphertext too short (need at least nonce + 1 byte)".into(),
			));
		}

		let (nonce_bytes, ciphertext) = combined.split_at(12);
		let nonce = Nonce::from_slice(nonce_bytes);

		let plaintext = self.cipher.decrypt(nonce, ciphertext).map_err(|_| {
			OversyncError::Internal("decrypt failed (wrong key or corrupted data)".into())
		})?;

		String::from_utf8(plaintext)
			.map_err(|e| OversyncError::Internal(format!("decrypt utf8: {e}")))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn encrypt_decrypt_roundtrip() {
		let store = AesGcmStore::from_passphrase("test-key-123");
		let secret = "postgres://admin:s3cret@db.prod:5432/app";
		let encrypted = store.encrypt(secret).unwrap();
		assert_ne!(encrypted, secret);
		let decrypted = store.decrypt(&encrypted).unwrap();
		assert_eq!(decrypted, secret);
	}

	#[test]
	fn different_passphrases_different_ciphertext() {
		let store1 = AesGcmStore::from_passphrase("key-1");
		let store2 = AesGcmStore::from_passphrase("key-2");
		let encrypted1 = store1.encrypt("secret").unwrap();
		let encrypted2 = store2.encrypt("secret").unwrap();
		assert_ne!(encrypted1, encrypted2);
	}

	#[test]
	fn wrong_key_fails_decrypt() {
		let store1 = AesGcmStore::from_passphrase("correct");
		let store2 = AesGcmStore::from_passphrase("wrong");
		let encrypted = store1.encrypt("secret").unwrap();
		let err = store2.decrypt(&encrypted).unwrap_err();
		assert!(err.to_string().contains("decrypt failed"));
	}

	#[test]
	fn empty_string_roundtrip() {
		let store = AesGcmStore::from_passphrase("key");
		let encrypted = store.encrypt("").unwrap();
		let decrypted = store.decrypt(&encrypted).unwrap();
		assert_eq!(decrypted, "");
	}

	#[test]
	fn unicode_roundtrip() {
		let store = AesGcmStore::from_passphrase("key");
		let secret = "пароль-密码-🔑";
		let decrypted = store.decrypt(&store.encrypt(secret).unwrap()).unwrap();
		assert_eq!(decrypted, secret);
	}

	#[test]
	fn each_encryption_produces_different_output() {
		let store = AesGcmStore::from_passphrase("key");
		let e1 = store.encrypt("same").unwrap();
		let e2 = store.encrypt("same").unwrap();
		assert_ne!(e1, e2); // different nonces
		assert_eq!(store.decrypt(&e1).unwrap(), "same");
		assert_eq!(store.decrypt(&e2).unwrap(), "same");
	}

	#[test]
	fn invalid_base64_fails() {
		let store = AesGcmStore::from_passphrase("key");
		let err = store.decrypt("not-valid-base64!!!").unwrap_err();
		assert!(err.to_string().contains("base64"));
	}

	#[test]
	fn too_short_ciphertext_fails() {
		let store = AesGcmStore::from_passphrase("key");
		let short = BASE64.encode(&[0u8; 5]);
		let err = store.decrypt(&short).unwrap_err();
		assert!(err.to_string().contains("too short"));
	}

	#[test]
	fn from_raw_key_works() {
		let key = [42u8; 32];
		let store = AesGcmStore::from_key(&key);
		let encrypted = store.encrypt("test").unwrap();
		assert_eq!(store.decrypt(&encrypted).unwrap(), "test");
	}

	#[test]
	fn credential_type_display() {
		assert_eq!(CredentialType::Password.to_string(), "password");
		assert_eq!(CredentialType::Token.to_string(), "token");
		assert_eq!(CredentialType::Keypair.to_string(), "keypair");
	}

	#[test]
	fn credential_type_serde_roundtrip() {
		let json = serde_json::to_string(&CredentialType::Token).unwrap();
		assert_eq!(json, "\"token\"");
		let back: CredentialType = serde_json::from_str(&json).unwrap();
		assert_eq!(back, CredentialType::Token);
	}
}
