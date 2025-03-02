use std::hash::{Hash, Hasher};

struct Person {
    id: u32,
    name: String,
    phone: u64,
}

impl Hash for Person {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.phone.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::hash::DefaultHasher;

    use super::*;

    #[test]
    fn hash_one_value() {
        let person1 = Person {
            id: 5,
            name: "Janet".to_string(),
            phone: 555_666_7777,
        };
        let person2 = Person {
            id: 5,
            name: "Bob".to_string(),
            phone: 555_666_7777,
        };

        //assert_eq!(calculate_hash(&person1), 1);
        assert_eq!(calculate_hash(&person1) % 1000, 1);
    }

    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
}
