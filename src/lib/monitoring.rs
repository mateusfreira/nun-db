use crate::bo::*;
use atomic_float::*;
use std::sync::atomic::Ordering;
impl Databases {
    pub fn update_query_time_moving_avg(&self, current: u128) {
        self.query_ema.write().unwrap().next(current as f64); //Not sure if this will be performant
    }

    pub fn get_query_time_moving_avg(&self) -> f64 {
        self.query_ema.read().unwrap().get()
    }

    pub fn update_replication_time_moving_avg(&self, current: u128) {
        self.replication_ema.write().unwrap().next(current as f64); //Not sure if this will be performant
    }

    pub fn get_replication_time_moving_avg(&self) -> f64 {
        self.replication_ema.read().unwrap().get()
    }

    pub fn get_monitoring_state(&self) -> String {
        format!(
            "replication_time_moving_avg: {:?}, get_query_time_moving_avg: {:?}",
            self.get_replication_time_moving_avg(),
            self.get_query_time_moving_avg(),
        )
    }
}

impl NunEma {
    pub fn new(period: u32) -> NunEma {
        let k = 2.0 / (period as f64 + 1.0);
        Self {
            k,
            init: false,
            ema: AtomicF64::new(0.0),
        }
    }

    pub fn get(&self) -> f64 {
        self.ema.load(Ordering::Relaxed)
    }

    fn next(&mut self, input: f64) -> f64 {
        if self.init != true {
            self.ema = AtomicF64::new(input);
            self.init = true;
            self.ema.load(Ordering::Relaxed)
        } else {
            let current_ema = self.ema.load(Ordering::Relaxed);
            let new_ema = (1_f64 - self.k) * current_ema + self.k * input;
            //@todo check this is ultra scale
            self.ema
                .compare_and_swap(current_ema, new_ema, Ordering::Release);
            new_ema
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_computate_properly() {
        let mut ema = NunEma::new(5);
        assert_eq!(ema.next(81.59), 81.59);
        assert_eq!(ema.next(81.06), 81.41333333333334);
        assert_eq!(ema.next(82.87), 81.8988888888889);
        assert_eq!(ema.next(83.00), 82.26592592592594);
        assert_eq!(ema.next(83.61), 82.71395061728396);
        assert_eq!(ema.next(83.15), 82.85930041152264);
        assert_eq!(ema.next(82.84), 82.8528669410151);
        assert_eq!(ema.next(83.99), 83.23191129401008);
        assert_eq!(ema.next(84.55), 83.67127419600672);
        assert_eq!(ema.next(84.36), 83.9008494640045);
        assert_eq!(ema.next(85.53), 84.44389964266966);
        assert_eq!(ema.next(86.54), 85.14259976177978);
        assert_eq!(ema.next(86.89), 85.7250665078532);
        assert_eq!(ema.next(200.00), 123.81671100523546);
        assert_eq!(ema.next(270.00), 172.54447400349034);
        assert_eq!(ema.next(5.00), 116.69631600232691);
    }
}
