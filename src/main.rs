use rusqlite::{Connection, Result};

#[derive(Debug)]
struct GraphNode {
    guid: Option<u64>,
    vector: Vec<u8>,
    adj_list: Vec<u8>,
}

fn create_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IndexTable (
            vertex_id INTEGER PRIMARY KEY,
            guid      INTEGER,
            vector    BLOB,
            adj_list  BLOB
        )",
        (),
    )?;
    println!("Created table");
    Ok(())
}

fn insert_graph_node(conn: &Connection, graph_node: GraphNode) -> Result<()> {
    conn.execute(
        "INSERT INTO IndexTable (guid, vector, adj_list) VALUES (?1, ?2, ?3)",
        (&graph_node.guid, &(graph_node.vector), &graph_node.adj_list),
    )?;
    dbg!("Inserted graph_node {:?}", graph_node);
    Ok(())
}

fn query_table(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT vertex_id, guid, vector, adj_list FROM IndexTable")?;
    let iter = stmt.query_map([], |row| {
        Ok(GraphNode {
            guid: row.get(1)?,
            vector: row.get(2)?,
            adj_list: row.get(3)?,
        })
    })?;

    for item in iter {
        println!("Found graph_node {:?}", item.unwrap());
    }
    Ok(())
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    create_table(&conn)?;



     let me = GraphNode {
        guid: Some(0),
        vector: vec![1, 2, 3],
        adj_list: vec![1, 2, 3],
    };
    insert_graph_node(&conn, me)?;
    query_table(&conn)?;
    Ok(())
}
