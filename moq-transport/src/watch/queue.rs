// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-FileCopyrightText: 2023-2024 Luke Curley and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use super::State;
use futures::channel::oneshot;
use std::collections::VecDeque;

pub struct Queue<T> {
    state: State<VecDeque<(T, Option<oneshot::Sender<()>>)>>, // store optional notifier per item
}

impl<T> Queue<T> {
    /// Push an item onto the queue. Returns Err(item) if the queue has been closed.
    pub fn push(&mut self, item: T) -> Result<(), T> {
        match self.state.lock_mut() {
            Some(mut state) => state.push_back((item, None)),
            None => return Err(item),
        };

        Ok(())
    }

    /// Push an item without panicking if the queue lock is poisoned.
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        match self.state.try_lock_mut() {
            Ok(Some(mut state)) => state.push_back((item, None)),
            Ok(None) => return Err(item),
            Err(_) => {
                tracing::error!("queue lock poisoned while pushing item");
                return Err(item);
            }
        };

        Ok(())
    }

    /// Pop an item from the queue, waiting if necessary.
    pub async fn pop(&mut self) -> Option<T> {
        loop {
            // Scope 1: try to pop an item
            {
                let queue = self.state.lock();
                if !queue.is_empty() {
                    // Take mutable access only in a block
                    if let Some((item, notifier)) = {
                        let mut state_mut = queue.into_mut()?;
                        state_mut.pop_front()
                    } {
                        if let Some(tx) = notifier {
                            let _ = tx.send(()); // notify waiter
                        }
                        return Some(item);
                    }
                }
            }

            // Scope 2: wait for modifications
            let queue = self.state.lock();
            queue.modified()?.await;
        }
    }

    /// Drop the state
    pub fn close(self) -> Vec<T> {
        // Drain the queue of any remaining entries
        let res = match self.state.lock_mut() {
            Some(mut queue) => queue.drain(..).map(|(item, _)| item).collect(),
            _ => Vec::new(),
        };

        // Prevent any new entries from being added
        drop(self.state);

        res
    }

    /// Push an item and wait until it is popped.
    /// Returns `Ok(())` if the item was successfully popped.
    /// Returns `Err(())` if the queue was closed before the item could be confirmed popped.
    #[allow(clippy::result_unit_err)]
    pub async fn push_and_wait_until_popped(&mut self, item: T) -> Result<(), ()> {
        // Create a oneshot channel
        let (tx, rx) = oneshot::channel();

        // Push the item along with the sender
        match self.state.lock_mut() {
            Some(mut state) => state.push_back((item, Some(tx))),
            None => return Err(()), // Queue already closed before push
        }

        // Wait until the item is popped.
        // If we receive Canceled, it means the sender was dropped without sending,
        // which indicates the queue was closed while we were waiting.
        rx.await.map_err(|_| ())
    }

    /// Split the queue into two handles that share the same underlying state.
    pub fn split(self) -> (Self, Self) {
        let state = self.state.split();
        (Self { state: state.0 }, Self { state: state.1 })
    }
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            state: State::new(Default::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::poll;

    #[tokio::test]
    async fn receipt_is_confirmed_only_when_its_item_is_popped() {
        let (mut producer, mut consumer) = Queue::default().split();
        producer.push("first").unwrap();
        let receipt = producer.push_and_wait_until_popped("second");
        tokio::pin!(receipt);
        assert!(poll!(&mut receipt).is_pending());
        assert_eq!(consumer.pop().await, Some("first"));
        assert!(poll!(&mut receipt).is_pending());
        assert_eq!(consumer.pop().await, Some("second"));
        assert_eq!(receipt.await, Ok(()));
    }

    #[tokio::test]
    async fn close_returns_pending_items_and_fails_their_receipts() {
        let (mut producer, consumer) = Queue::default().split();
        producer.push("first").unwrap();
        let receipt = producer.push_and_wait_until_popped("second");
        tokio::pin!(receipt);
        assert!(poll!(&mut receipt).is_pending());
        assert_eq!(consumer.close(), vec!["first", "second"]);
        assert_eq!(receipt.await, Err(()));
    }

    #[tokio::test]
    async fn cancelling_a_receipt_keeps_fifo_delivery_intact() {
        let (mut producer, mut consumer) = Queue::default().split();
        {
            let receipt = producer.push_and_wait_until_popped("first");
            tokio::pin!(receipt);
            assert!(poll!(&mut receipt).is_pending());
        }
        producer.push("second").unwrap();
        assert_eq!(consumer.pop().await, Some("first"));
        assert_eq!(consumer.pop().await, Some("second"));
        drop(producer);
        assert_eq!(consumer.pop().await, None);
    }
}
