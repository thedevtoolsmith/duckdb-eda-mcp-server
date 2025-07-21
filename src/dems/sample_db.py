import json
import os

import duckdb


def create_db_with_sample_data(db_path: str):
    """
    Create a sample DuckDB database with test data at the beginning of the test session.
    The database will be automatically cleaned up after all tests are run.
    """

    # Remove the database file if it already exists
    if os.path.exists(db_path):
        os.remove(db_path)

    # Connect to the database
    conn = duckdb.connect(db_path)

    # Create the tables
    conn.execute("""
                 CREATE TABLE authors
                 (
                     author_id  INTEGER PRIMARY KEY,
                     name       VARCHAR(100) NOT NULL,
                     birth_year INTEGER,
                     country    VARCHAR(50)
                 )
                 """)

    conn.execute("""
                 CREATE TABLE books
                 (
                     book_id          INTEGER PRIMARY KEY,
                     title            VARCHAR(200) NOT NULL,
                     author_id        INTEGER      NOT NULL,
                     publication_year INTEGER,
                     genre            VARCHAR(50),
                     price            DECIMAL(10, 2),
                     FOREIGN KEY (author_id) REFERENCES authors (author_id)
                 )
                 """)

    conn.execute("""
                 CREATE TABLE reviews
                 (
                     review_id     INTEGER PRIMARY KEY,
                     book_id       INTEGER NOT NULL,
                     reviewer_name VARCHAR(100),
                     rating        INTEGER CHECK (rating BETWEEN 1 AND 5),
                     review_text   TEXT,
                     review_date   DATE,
                     FOREIGN KEY (book_id) REFERENCES books (book_id)
                 )
                 """)

    # Insert sample data for authors
    authors_data = [
        (1, "Jane Austen", 1775, "United Kingdom"),
        (2, "George Orwell", 1903, "United Kingdom"),
        (3, "Gabriel García Márquez", 1927, "Colombia"),
        (4, "Haruki Murakami", 1949, "Japan"),
        (5, "Chimamanda Ngozi Adichie", 1977, "Nigeria"),
    ]

    conn.executemany(
        """
        INSERT INTO authors (author_id, name, birth_year, country)
        VALUES (?, ?, ?, ?)
        """,
        authors_data,
    )

    # Insert sample data for books
    books_data = [
        (1, "Pride and Prejudice", 1, 1813, "Classic", 12.99),
        (2, "1984", 2, 1949, "Dystopian", 14.50),
        (3, "One Hundred Years of Solitude", 3, 1967, "Magical Realism", 15.75),
        (4, "Norwegian Wood", 4, 1987, "Fiction", 13.25),
        (5, "Half of a Yellow Sun", 5, 2006, "Historical Fiction", 16.00),
        (6, "Sense and Sensibility", 1, 1811, "Classic", 11.99),
        (7, "Animal Farm", 2, 1945, "Political Satire", 10.50),
        (8, "Kafka on the Shore", 4, 2002, "Magical Realism", 14.75),
    ]

    conn.executemany(
        """
        INSERT INTO books (book_id, title, author_id, publication_year, genre, price)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        books_data,
    )

    # Insert sample data for reviews
    reviews_data = [
        (
            1,
            1,
            "John Smith",
            5,
            "A timeless classic that still resonates today.",
            "2020-05-15",
        ),
        (
            2,
            1,
            "Emily Johnson",
            4,
            "Beautifully written, but pacing is a bit slow.",
            "2021-02-10",
        ),
        (3, 2, "Michael Brown", 5, "Disturbing and prophetic. A must-read.", "2019-11-20"),
        (4, 3, "Sarah Davis", 5, "Breathtaking prose and storytelling.", "2020-08-05"),
        (
            5,
            4,
            "David Wilson",
            4,
            "Murakami at his best. Haunting and beautiful.",
            "2021-01-30",
        ),
        (
            6,
            5,
            "Lisa Anderson",
            5,
            "Powerful and moving historical narrative.",
            "2020-07-22",
        ),
        (
            7,
            2,
            "Robert Thomas",
            3,
            "Interesting concepts but found it depressing.",
            "2020-12-18",
        ),
        (8, 7, "Jennifer Lee", 4, "Brilliant allegory. Short but impactful.", "2021-03-05"),
        (
            9,
            8,
            "Kevin Chen",
            5,
            "A mesmerizing journey into magical realism.",
            "2020-09-12",
        ),
        (
            10,
            6,
            "Amanda Martin",
            4,
            "A delightful read from a master novelist.",
            "2021-04-20",
        ),
    ]

    conn.executemany(
        """
        INSERT INTO reviews (review_id, book_id, reviewer_name, rating, review_text, review_date)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        reviews_data,
    )

    conn.close()


def create_sample_csv(csv_path: str):
    with open(csv_path, "w") as csv_file:
        csv_file.write("id,name,value\n")
        csv_file.write("1,test1,100\n")
        csv_file.write("2,test2,200\n")


def create_sample_json(json_path: str):
    json_data = [{"id": 1, "name": "json1", "value": 300}, {"id": 2, "name": "json2", "value": 400}]
    with open(json_path, "w") as json_file:
        json.dump(json_data, json_file)
