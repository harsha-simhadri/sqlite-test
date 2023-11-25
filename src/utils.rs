use rand::Rng;
use rand_distr::{Distribution, Normal};

pub fn vec_u32_to_u8(vec: &Vec<u32>) -> Vec<u8> {
    let mut vec_u8: Vec<u8> = vec![];
    for x in vec {
        vec_u8.extend_from_slice(&x.to_le_bytes());
    }
    vec_u8
}

pub fn vec_u8_to_u32(vec: &Vec<u8>) -> Vec<u32> {
    let mut vec_u32: Vec<u32> = vec![];
    for i in 0..vec.len() / 4 {
        let mut bytes: [u8; 4] = [0; 4];
        bytes.copy_from_slice(&vec[i * 4..(i + 1) * 4]);
        vec_u32.push(u32::from_le_bytes(bytes));
    }
    vec_u32
}

pub fn vec_u64_to_set_str(adj_list: &Vec<u64>) -> String {
    let mut adj_list_str = "(".to_string();
    let mut iter = adj_list.iter().peekable();
    while let Some(x) = iter.next() {
        adj_list_str.push_str(&format!(
            "{}{}",
            x,
            if iter.peek().is_some() { ", " } else { ")" }
        ));
    }
    adj_list_str
}

pub fn generate_random_vecs(ndim: usize, nvec: usize, radius: f32) -> Vec<Vec<u8>> {
    assert!(radius > 0.0 && radius < 127.0);
    let mut thr_rng = rand::thread_rng();
    let normal: Normal<f32> = Normal::new(0.0, 1.0).unwrap();
    let mut data: Vec<Vec<u8>> = Vec::with_capacity(nvec);
    for _ in 0..nvec {
        let vec: Vec<f32> = (0..ndim).map(|_| normal.sample(&mut thr_rng)).collect();
        let norm = vec.iter().fold(0.0, |acc, x| acc + x * x).sqrt();
        data.push(
            vec.iter()
                .map(|x| (((*x * radius) / norm) + 127.0) as u8)
                .collect(),
        );
    }
    data
}

pub fn generate_random_adj_list(nvec: usize, degree: usize, max_idx: usize) -> Vec<Vec<u32>> {
    let mut thr_rng = rand::thread_rng();
    (0..nvec)
        .map(|_| {
            (0..degree)
                .map(|_| thr_rng.gen_range(0..max_idx) as u32)
                .collect()
        })
        .collect()
}
