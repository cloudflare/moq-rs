//! Speech activity state machine for simulating realistic publisher behavior.
//!
//! State transitions:
//!   SILENT → SPEECH_START → SPEAKING → SPEECH_ENDED → SILENT
//!
//! Property values:
//!   SILENT:       0
//!   SPEECH_START: 2  (first 300ms of speech - "burst" to indicate start)
//!   SPEAKING:     1  (ongoing speech)
//!   SPEECH_ENDED: 1  (keep sending 1 until next group starts)
//!                 0  (send 0 on first group after speech ended)

use rand::Rng;
use std::time::{Duration, Instant};

/// Speech activity states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeechState {
    Silent,
    SpeechStart,
    Speaking,
    SpeechEnded,
}

/// Speech activity simulator
pub struct SpeechSimulator {
    state: SpeechState,
    state_start: Instant,
    speech_duration: Duration,
    silence_duration: Duration,
    speech_start_duration: Duration,
    /// Whether we've sent the final 0 after speech ended
    sent_final_zero: bool,
}

impl SpeechSimulator {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            state: SpeechState::Silent,
            state_start: Instant::now(),
            speech_duration: Duration::from_secs(0),
            silence_duration: Self::random_silence_duration(&mut rng),
            speech_start_duration: Duration::from_millis(300),
            sent_final_zero: true,
        }
    }

    /// Get the current property value to send
    pub fn current_value(&self) -> u8 {
        match self.state {
            SpeechState::Silent => 0,
            SpeechState::SpeechStart => 2,
            SpeechState::Speaking => 1,
            SpeechState::SpeechEnded => {
                if self.sent_final_zero {
                    0
                } else {
                    1
                }
            }
        }
    }

    /// Get the current state
    pub fn state(&self) -> SpeechState {
        self.state
    }

    /// Called when a new group is about to be sent. Updates state and returns the value to send.
    pub fn tick(&mut self) -> u8 {
        let mut rng = rand::thread_rng();
        let elapsed = self.state_start.elapsed();

        match self.state {
            SpeechState::Silent => {
                if elapsed >= self.silence_duration {
                    // Start speaking
                    self.state = SpeechState::SpeechStart;
                    self.state_start = Instant::now();
                    self.speech_duration = Self::random_speech_duration(&mut rng);
                    self.sent_final_zero = false;
                }
            }
            SpeechState::SpeechStart => {
                if elapsed >= self.speech_start_duration {
                    // Transition to normal speaking
                    self.state = SpeechState::Speaking;
                    // Don't reset state_start - we want total speech time
                }
                // Check if speech should end during start phase
                if self.state_start.elapsed() >= self.speech_duration {
                    self.state = SpeechState::SpeechEnded;
                    self.state_start = Instant::now();
                }
            }
            SpeechState::Speaking => {
                // Check total time since speech started (including start phase)
                let total_speech_time = Instant::now().duration_since(
                    self.state_start - self.speech_start_duration.min(elapsed)
                );
                if elapsed >= self.speech_duration.saturating_sub(self.speech_start_duration) {
                    self.state = SpeechState::SpeechEnded;
                    self.state_start = Instant::now();
                }
            }
            SpeechState::SpeechEnded => {
                if !self.sent_final_zero {
                    // This tick sends the final 1, next tick will be 0
                    self.sent_final_zero = true;
                } else {
                    // Transition back to silent
                    self.state = SpeechState::Silent;
                    self.state_start = Instant::now();
                    self.silence_duration = Self::random_silence_duration(&mut rng);
                }
            }
        }

        self.current_value()
    }

    /// Random speech duration: 2-8 seconds
    fn random_speech_duration(rng: &mut impl Rng) -> Duration {
        Duration::from_millis(rng.gen_range(2000..8000))
    }

    /// Random silence duration: 1-5 seconds
    fn random_silence_duration(rng: &mut impl Rng) -> Duration {
        Duration::from_millis(rng.gen_range(1000..5000))
    }
}

impl Default for SpeechSimulator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let sim = SpeechSimulator::new();
        assert_eq!(sim.state(), SpeechState::Silent);
        assert_eq!(sim.current_value(), 0);
    }

    #[test]
    fn test_speech_start_value() {
        let mut sim = SpeechSimulator::new();
        sim.state = SpeechState::SpeechStart;
        assert_eq!(sim.current_value(), 2);
    }

    #[test]
    fn test_speaking_value() {
        let mut sim = SpeechSimulator::new();
        sim.state = SpeechState::Speaking;
        assert_eq!(sim.current_value(), 1);
    }
}
