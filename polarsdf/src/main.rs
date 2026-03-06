//command-line tool that reads a CSV file and prints the contents of the file as a DataFrame
use clap::{CommandFactory, Parser};
use polars::prelude::*;
const CSV_FILE: &str = "src/data/global-life-expt-2022.csv";

#[derive(Parser)]
//add extended help
#[clap(
    version = "1.0",
    author = "Noah Gift",
    about = "A command-line tool that reads a CSV file and prints the contents of the file as a DataFrame",
    after_help = "Example: cargo run -- print --rows 3"
)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
}

#[derive(Parser)]
enum Commands {
    Print {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
        #[clap(long, default_value = "10")]
        rows: usize,
    },
    Describe {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
    },
    Schema {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
    },
    Shape {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
    },
    Sort {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
        #[clap(long, default_value = "2020")]
        year: String,
        #[clap(long, default_value = "10")]
        rows: usize,
        #[clap(long, default_value = "true")]
        order: bool,
        #[clap(long, default_value = "year")]
        column: String,
    },
    Filter {
        #[clap(long, default_value = CSV_FILE)]
        path: String,
        #[clap(long)]
        query: String,
        #[clap(long)]
        table: String,
    },
}

fn main() {
    let args = Cli::parse();
    match args.command {
        Some(Commands::Print { path, rows }) => {
            let df = polarsdf::read_csv(&path);
            println!("{:?}", df.head(Some(rows)));
        }
        Some(Commands::Describe { path }) => {
            let df = polarsdf::read_csv(&path);
            println!("{:?}", df);
        }
        Some(Commands::Schema { path }) => {
            let df = polarsdf::read_csv(&path);
            println!("{:?}", df.schema());
        }
        Some(Commands::Shape { path }) => {
            let df = polarsdf::read_csv(&path);
            println!("{:?}", df.shape());
        }
        Some(Commands::Sort {
            path,
            year,
            rows,
            order,
            column,
        }) => {
            let df = polarsdf::read_csv(&path);
            let country_column_name = "Country Name";
            // Exercise 1: Modify the Sort command to sort based on any given column name, not just "year".
            // Test your implementation.
            let column_names = df.get_column_names();
            let sort_column = column != "year" && column_names.iter().any(|f| **f == column);
            let sort_by = if sort_column { column } else { year.clone() };
            let columns = match sort_column {
                true => Vec::from([country_column_name, &year, &sort_by]),
                false => Vec::from([country_column_name, &year]),
            };
            //select the country column and the year string passed in and return a new dataframe
            let df2 = df.select(columns).unwrap();
            //drop any rows with null values and return a new dataframe
            let df2: DataFrame = df2.drop_nulls::<String>(None).unwrap();
            //sort the dataframe by the year column and by order passed in
            let df2: DataFrame = df2
                .sort(
                    [&sort_by],
                    SortMultipleOptions::new().with_order_descending(order),
                )
                .unwrap();

            //print the first "rows" of the dataframe
            println!("{:?}", df2.head(Some(rows)));
        }
        Some(Commands::Filter { path, query, table }) => {
            // println!("{:?}", vec!([path, year, column, condition]));
            let mut cli = Cli::command();
            let df = polarsdf::read_csv(&path);
            let mut sql = polars::sql::SQLContext::new();
            sql.register(&table, df.lazy());
            let res = sql.execute(&query);

            match res {
                Err(err) => {
                    cli.error(clap::error::ErrorKind::Io, err);
                }
                Ok(lf) => {
                    let results = lf.collect();
                    println!("{:?}", results);
                }
            }
        }
        None => {
            println!("No subcommand was used");
        }
    }
}
