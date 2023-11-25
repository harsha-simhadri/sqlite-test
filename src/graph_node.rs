#[derive(Debug)]
pub struct GraphNode {
    pub guid: Option<u64>,
    pub vector: Vec<u8>,
    pub adj_list: Vec<u32>,
}

impl std::fmt::Display for GraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut adj_list_str = String::new();
        for x in &self.adj_list {
            adj_list_str.push_str(&format!("{}, ", x));
        }
        write!(
            f,
            "guid: {:?}, vector: {:?}, adj_list: {:?}",
            self.guid, self.vector, adj_list_str
        )
    }
}
