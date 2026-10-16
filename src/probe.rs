use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blake2::{Blake2b, Digest, digest::consts::U32};
use katzenpost_thin_client::{ThinClient, ThinClientError};
use rand::RngCore;
use tokio::time::timeout;

use crate::metrics::Metrics;

#[derive(Debug)]
pub enum Error {
    Thin(ThinClientError),
    Timeout,
    Mismatch,
}

impl From<ThinClientError> for Error {
    fn from(e: ThinClientError) -> Self {
        Error::Thin(e)
    }
}

impl Error {
    /// The error variant's name, e.g. "BoxNotFound".
    fn label(&self) -> String {
        match self {
            Error::Thin(e) => {
                let s = format!("{e:?}");
                s.split(['(', ' ', '{']).next().unwrap_or("Unknown").to_string()
            }
            Error::Timeout => "Timeout".into(),
            Error::Mismatch => "Mismatch".into(),
        }
    }
}

struct Channel {
    write_cap: Vec<u8>,
    read_cap: Vec<u8>,
    first_index: Vec<u8>,
}

pub struct Probe {
    pub client: Arc<ThinClient>,
    pub metrics: Arc<Metrics>,
    pub timeout: Duration,
    pub box_len: usize,
    pub boxes: usize,
}

impl Probe {
    pub async fn cycle(&self) -> Result<(), Error> {
        let one = vec![random(self.box_len)];
        let ch = self.new_channel().await?;
        if self.timed("write_box", self.write(&ch, &one)).await {
            self.timed("read_box", self.read(&ch, &one)).await;
        }

        let many: Vec<Vec<u8>> = (0..self.boxes).map(|_| random(self.box_len)).collect();
        let ch = self.new_channel().await?;
        if self.timed("write_n_boxes", self.write(&ch, &many)).await {
            self.timed("read_n_boxes", self.read(&ch, &many)).await;
        }

        let payload = many.concat();
        let ch = self.new_channel().await?;
        if self.timed("copy_n_boxes", self.copy(&ch, &payload)).await {
            self.timed("read_copied_boxes", self.read_bytes(&ch, &payload)).await;
        }
        Ok(())
    }

    async fn timed(&self, op: &str, f: impl Future<Output = Result<(), Error>>) -> bool {
        let start = Instant::now();
        let r = f.await;
        let d = start.elapsed();
        match &r {
            Ok(()) => {
                log::info!("{op}: {d:.1?}");
                self.metrics.record(op, Ok(d));
            }
            Err(e) => {
                log::warn!("{op}: failed after {d:.1?}: {e:?}");
                self.metrics.record(op, Err(&e.label()));
            }
        }
        r.is_ok()
    }

    async fn new_channel(&self) -> Result<Channel, Error> {
        let seed: [u8; 32] = rand::random();
        let kp = self.client.new_keypair(&seed).await?;
        Ok(Channel { write_cap: kp.write_cap, read_cap: kp.read_cap, first_index: kp.first_message_index })
    }

    /// Writes `boxes` into `ch` one after another.
    async fn write(&self, ch: &Channel, boxes: &[Vec<u8>]) -> Result<(), Error> {
        let mut index = ch.first_index.clone();
        for plaintext in boxes {
            index = self.write_box(&ch.write_cap, &index, plaintext).await?;
        }
        Ok(())
    }

    async fn write_box(&self, write_cap: &[u8], index: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let w = self.client.encrypt_write(plaintext, write_cap, index).await?;
        let send = self.client.start_resending_encrypted_message(
            None, Some(write_cap), None, Some(0),
            &w.envelope_descriptor, &w.message_ciphertext, &w.envelope_hash,
        );
        self.deadline(&w.envelope_hash, send).await?;
        Ok(w.next_message_box_index)
    }

    async fn read_box(&self, read_cap: &[u8], index: &[u8]) -> Result<Vec<u8>, Error> {
        let r = self.client.encrypt_read(read_cap, index).await?;
        let send = self.client.start_resending_encrypted_message(
            Some(read_cap), None, Some(index), Some(0),
            &r.envelope_descriptor, &r.message_ciphertext, &r.envelope_hash,
        );
        Ok(self.deadline(&r.envelope_hash, send).await?.plaintext)
    }

    /// Reads `expected.len()` boxes from `ch` one after another and checks them.
    async fn read(&self, ch: &Channel, expected: &[Vec<u8>]) -> Result<(), Error> {
        let mut index = ch.first_index.clone();
        for want in expected {
            if self.read_box(&ch.read_cap, &index).await? != *want {
                return Err(Error::Mismatch);
            }
            index = self.client.next_message_box_index(&index).await?;
        }
        Ok(())
    }

    /// Reads boxes from `ch` until their concatenation is as long as `expected`.
    async fn read_bytes(&self, ch: &Channel, expected: &[u8]) -> Result<(), Error> {
        let mut got = Vec::new();
        let mut index = ch.first_index.clone();
        while got.len() < expected.len() {
            got.extend(self.read_box(&ch.read_cap, &index).await?);
            index = self.client.next_message_box_index(&index).await?;
        }
        if got != expected {
            return Err(Error::Mismatch);
        }
        Ok(())
    }

    /// Writes `payload` into `dest` with the COPY command: the boxes go to a
    /// temporary channel and the courier copies them into `dest`.
    async fn copy(&self, dest: &Channel, payload: &[u8]) -> Result<(), Error> {
        let envelopes = self
            .client
            .create_courier_envelopes_from_payload(payload, &dest.write_cap, &dest.first_index, true, true)
            .await?
            .envelopes;
        let temp = self.new_channel().await?;
        self.write(&temp, &envelopes).await?;
        let copy = self.client.start_resending_copy_command(&temp.write_cap, None, None);
        match timeout(self.timeout, copy).await {
            Ok(r) => Ok(r?),
            Err(_) => {
                let hash: [u8; 32] = Blake2b::<U32>::digest(&temp.write_cap).into();
                let _ = self.client.cancel_resending_copy_command(&hash).await;
                Err(Error::Timeout)
            }
        }
    }

    /// Waits for a resending request, cancelling it in kpclientd on timeout
    /// so the daemon does not keep resending it.
    async fn deadline<T>(
        &self,
        envelope_hash: &[u8; 32],
        f: impl Future<Output = Result<T, ThinClientError>>,
    ) -> Result<T, Error> {
        match timeout(self.timeout, f).await {
            Ok(r) => Ok(r?),
            Err(_) => {
                let _ = self.client.cancel_resending_encrypted_message(envelope_hash).await;
                Err(Error::Timeout)
            }
        }
    }
}

fn random(len: usize) -> Vec<u8> {
    let mut v = vec![0; len];
    rand::thread_rng().fill_bytes(&mut v);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_labels_are_variant_names() {
        assert_eq!(Error::Thin(ThinClientError::BoxNotFound).label(), "BoxNotFound");
        assert_eq!(Error::Thin(ThinClientError::Other("x".into())).label(), "Other");
        let copy = ThinClientError::CopyCommandFailed { replica_error_code: 10, failed_envelope_index: 1 };
        assert_eq!(Error::Thin(copy).label(), "CopyCommandFailed");
        assert_eq!(Error::Timeout.label(), "Timeout");
    }
}
