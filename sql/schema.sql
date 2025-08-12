PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS prices(
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    close REAL NOT NULL,
    PRIMARY KEY(date, ticker)
);
CREATE TABLE IF NOT EXISTS weights(
    valid_from TEXT NOT NULL,
    ticker TEXT NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY(valid_from, ticker)
);