#[derive(Copy, Clone, Debug)]
pub struct MovementPolicy {
    pub allow_diagonals: bool,
    pub allow_corner_cut: bool,
    pub unit_radius_tiles: i32,
}

impl Default for MovementPolicy {
    fn default() -> Self {
        Self { allow_diagonals: true, allow_corner_cut: false, unit_radius_tiles: 1 }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct Offset(pub i32, pub i32);

impl MovementPolicy {
    pub fn neighbor_offsets(&self) -> &'static [Offset] {
        const CARD: [Offset; 4] = [Offset(1,0), Offset(-1,0), Offset(0,1), Offset(0,-1)];
        const ALL: [Offset; 8] = [
            Offset(1,0), Offset(-1,0), Offset(0,1), Offset(0,-1),
            Offset(1,1), Offset(1,-1), Offset(-1,1), Offset(-1,-1),
        ];
        if self.allow_diagonals { &ALL } else { &CARD }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neighbor_offsets_no_diagonals() {
        let p = MovementPolicy { allow_diagonals: false, allow_corner_cut: false, unit_radius_tiles: 1 };
        let offs = p.neighbor_offsets();
        assert_eq!(offs.len(), 4);
        assert!(offs.contains(&Offset(1,0)));
        assert!(offs.contains(&Offset(-1,0)));
        assert!(offs.contains(&Offset(0,1)));
        assert!(offs.contains(&Offset(0,-1)));
    }

    #[test]
    fn neighbor_offsets_with_diagonals() {
        let p = MovementPolicy { allow_diagonals: true, allow_corner_cut: false, unit_radius_tiles: 1 };
        let offs = p.neighbor_offsets();
        assert_eq!(offs.len(), 8);
        assert!(offs.contains(&Offset(1,1)));
        assert!(offs.contains(&Offset(-1,-1)));
    }
}
