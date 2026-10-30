# csvql plugin

A skill that has Claude answer questions about CSV and TSV files with [csvql](https://github.com/melihbirim/csvql): SQL run directly on the file, read-only, no import, no database. Claude checks the schema first, keeps results small, and never pastes a large file into context.

Needs the `csvql` binary on PATH:

```bash
brew install melihbirim/csvql/csvql
```

or `pip install csvql-query`, `npm install -g csvql-query`, or a binary from the [releases](https://github.com/melihbirim/csvql/releases).

Install the plugin:

```bash
claude plugin marketplace add melihbirim/csvql
claude plugin install csvql@csvql
```

Then ask things like "top 10 customers by revenue in sales.csv" or "how many rows in events.csv have status error".
