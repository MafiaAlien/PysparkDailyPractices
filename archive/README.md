# archive

Ad-hoc practice scripts from before the daily workflow existed — written
against whatever felt worth drilling that afternoon, with no problem
statement, no planted trap, no blind review, and no reference answer.

They are kept for continuity, not as a sample of the work. The structured
practice lives in [`../days/`](../days/), and
[`../README.md`](../README.md) explains how a day is built.

One of them (`Gemini_pyspark_tests_06142026.py`) ends on a blocking
`input()` that holds the process open so the Spark UI stays reachable at
`localhost:4040`. It will not exit on its own.
