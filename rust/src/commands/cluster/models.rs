#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ChunkId {
    pub x: i32,
    pub z: i32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PlaneId(pub i32);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TileId(pub u64);

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClusterId(pub u64);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum EdgeKind {
    Intra,
    Inter,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Entrance {
    pub id: u64,
    pub from_cluster: ClusterId,
    pub to_cluster: ClusterId,
    pub from_tile: TileId,
    pub to_tile: TileId,
    pub cost: i32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Edge {
    pub id: u64,
    pub a: TileId,
    pub b: TileId,
    pub weight: i32,
    pub kind: EdgeKind,
}
