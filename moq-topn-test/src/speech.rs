//! Speech activity state machine for simulating realistic publisher behavior.
//!
//! Tick-based state machine matching moqx's parameters:
//!   - p(start speaking) = 0.03 per tick when silent
//!   - Speech start value = 2 (highest priority, 1 tick)
//!   - Speaking value = 1 (90-300 ticks at 30Hz = 3-10s)
//!   - Silent value = 0 (150-900 ticks at 30Hz = 5-30s)
//!
//! State transitions:
//!   SILENT → SPEECH_START → SPEAKING → SILENT

use rand::Rng;

/// Speech activity states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeechState {
    Silent,
    SpeechStart,
    Speaking,
}

/// Tick-based speech activity simulator matching moqx behavior.
pub struct SpeechSimulator {
    state: SpeechState,
    ticks_in_state: u64,
    speaking_duration_ticks: u64,
}

impl SpeechSimulator {
    pub fn new() -> Self {
        Self {
            state: SpeechState::Silent,
            ticks_in_state: 0,
            speaking_duration_ticks: 0,
        }
    }

    pub fn current_value(&self) -> u8 {
        match self.state {
            SpeechState::Silent => 0,
            SpeechState::SpeechStart => 2,
            SpeechState::Speaking => 1,
        }
    }

    pub fn state(&self) -> SpeechState {
        self.state
    }

    /// Called once per group interval. Updates state and returns the value to send.
    pub fn tick(&mut self) -> u8 {
        let mut rng = rand::thread_rng();

        match self.state {
            SpeechState::Silent => {
                self.ticks_in_state += 1;
                // p(start speaking) = 0.03 per tick
                if rng.gen::<f64>() < 0.03 {
                    self.state = SpeechState::SpeechStart;
                    self.ticks_in_state = 0;
                    // Speaking duration: 90-300 ticks (3-10s at 30Hz)
                    self.speaking_duration_ticks = rng.gen_range(90..=300);
                }
            }
            SpeechState::SpeechStart => {
                // Speech start lasts exactly 1 tick
                self.state = SpeechState::Speaking;
                self.ticks_in_state = 0;
            }
            SpeechState::Speaking => {
                self.ticks_in_state += 1;
                if self.ticks_in_state >= self.speaking_duration_ticks {
                    self.state = SpeechState::Silent;
                    self.ticks_in_state = 0;
                }
            }
        }

        self.current_value()
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

    #[test]
    fn test_state_transitions() {
        let mut sim = SpeechSimulator::new();
        // Run many ticks; should eventually enter speaking state
        let mut saw_speech = false;
        for _ in 0..10000 {
            sim.tick();
            if sim.state() == SpeechState::Speaking || sim.state() == SpeechState::SpeechStart {
                saw_speech = true;
                break;
            }
        }
        assert!(saw_speech, "should eventually start speaking");
    }
}
