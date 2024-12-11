import argparse
import sqlite3
import typing as t
from pathlib import Path
import logging
import pytest


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)


def display_plane(staff: t.List[t.Dict[str, t.Any]]) -> None:
    if staff:
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 12
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^12} |'.format(
                "No",
                "Destination",
                "Race number",
                "Plane type"
            )
        )
        print(line)

        for idx, planes in enumerate(staff, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>12} |'.format(
                    idx,
                    planes.get('race', ''),
                    planes.get('number', ''),
                    planes.get('type', 0)
                )
            )
            print(line)

    else:
        logger.info("The flight list is empty.")


def create_db(database_path: Path) -> None:
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cities (
                race_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_name INTEGER NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS races (
                race_id INTEGER PRIMARY KEY AUTOINCREMENT,
                race_name TEXT NOT NULL,
                number_name INTEGER NOT NULL,
                type_name INTEGER NOT NULL,
                FOREIGN KEY(race_name) REFERENCES cities(race_name)
            )
            """
        )

        conn.close()
        logger.info("Database created successfully.")
    except sqlite3.Error as e:
        logger.error(f"Error creating database: {e}")


def add_plane(
    database_path: Path,
    race: str,
    number: int,
    type: int
) -> None:
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT race_id FROM cities WHERE race_name = ?
            """,
            (race,)
        )
        row = cursor.fetchone()
        if row is None:
            cursor.execute(
                """
                INSERT INTO cities (race_name) VALUES (?)
                """,
                (race,)
            )
            race_id = cursor.lastrowid
        else:
            race_id = row[0]

        cursor.execute(
            """
            INSERT INTO races (race_name, number_name, type_name)
            VALUES (?, ?, ?)
            """,
            (race, number, type)
        )

        conn.commit()
        conn.close()
        logger.info(f"Plane added successfully: {race}, {number}, {type}")
    except sqlite3.Error as e:
        logger.error(f"Error adding plane: {e}")


def select_allplanes(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    try:
        conn = sqlite3.connect(database_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT races.race_name, races.number_name, races.type_name
            FROM races
            INNER JOIN cities ON cities.race_id = races.race_id
            """
        )
        rows = cursor.fetchall()

        conn.close()
        return [
            {
                "race": row[0],
                "number": row[1],
                "type": row[2],
            }
            for row in rows
        ]
    except sqlite3.Error as e:
        logger.error(f"Error selecting planes: {e}")
        return []


def main(command_line=None):
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.home() / "workers.db"),
        help="The database file name"
    )

    parser = argparse.ArgumentParser("workers")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )

    subparsers = parser.add_subparsers(dest="command")

    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new race"
    )
    add.add_argument(
        "-r",
        "--race",
        action="store",
        required=True,
        help="The city where plane will go"
    )
    add.add_argument(
        "-n",
        "--number",
        action="store",
        type=int,
        required=True,
        help="The number of race"
    )
    add.add_argument(
        "-t",
        "--type",
        action="store",
        type=int,
        required=True,
        help="The type of plane"
    )

    display = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all races"
    )

    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the races"
    )

    args = parser.parse_args(command_line)

    db_path = Path(args.db)
    create_db(db_path)

    if args.command == "add":
        add_plane(db_path, args.race, args.number, args.type)

    elif args.command == "display":
        display_plane(select_allplanes(db_path))


@pytest.fixture
def db_path():
    path = Path("test_planes.db")
    create_db(path)
    yield path
    if path.exists():
        path.unlink()


def test_create_db(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cities'")
    assert cursor.fetchone() is not None, "Table 'cities' not created."

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='races'")
    assert cursor.fetchone() is not None, "Table 'races' not created."

    conn.close()


def test_add_plane(db_path):
    add_plane(db_path, "Novoalexandrovsk", 3234, 62)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT race_name, number_name, type_name FROM races\
                    WHERE race_name = 'Novoalexandrovsk'")
    race = cursor.fetchone()

    assert race is not None, "Race was not added."
    assert race[0] == "Novoalexandrovsk", "Incorrect race name."
    assert race[1] == 3234, "Incorrect race number."
    assert race[2] == 62, "Incorrect plane type."

    conn.close()


def test_select_allplanes(db_path):
    add_plane(db_path, "Novoalexandrovsk", 3234, 62)
    add_plane(db_path, "Stavropol", 9120, 82)

    planes = select_allplanes(db_path)
    assert len(planes) == 2, "Некорректное число рейсов."

    assert planes[0]["race"] == "Novoalexandrovsk", "Некорректное название рейса."
    assert planes[0]["number"] == 3234, "Некорректный номер рейса."
    assert planes[0]["type"] == 62, "Некорректный тип самолета."

    assert planes[1]["race"] == "Stavropol", "Некорректное название рейса."
    assert planes[1]["number"] == 9120, "Некорректный номер рейса."
    assert planes[1]["type"] == 82, "Некорректный тип самолета."


if __name__ == "__main__":
    main()