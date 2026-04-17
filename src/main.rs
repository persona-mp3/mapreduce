use mapreduce;
// Paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

fn main() {
    let vec_key_values = mapreduce::worker_map(
        "test_file_name",
        "
        Topic: Map Reduce, simplified Data Processing on Large Clusters
            Jeffery Dean and Sanjay Ghemawat
         MapReduce is a programming and an association implementation 
         for generating and processing large datasets 
         Data
         React 
         Data
         Data, Backend Data, Data, Data
        ",
    );

    // SO while reading the paper, I'm trying to understand the
    // fn Reduce(k: &str, values: Vec<&str>) -> i32
    // So for instance, if we get a map_result of []
    // Data: 1, Data: 1, Data: 1, Data: 1, Data:1, Data: 1
    // We select, the `Data` and all its values
    //  <"Data", ["1",  "1",  "1",  "1", "1",  "1"]>
    //  Reduce( "Data", ["1",  "1",  "1",  "1", "1",  "1"]) -> 6

    for key_value in vec_key_values {
        println!(" {:?} ", key_value);
    }
}
