# Example Onedata Metadata API Usage

This script implements [producer-consumer pattern](https://betterprogramming.pub/hands-on-go-concurrency-the-producer-consumer-pattern-c42aab4e3bd2) to concurrently attach metadata to Onedata files using [the API](https://onedata.org/#/home/api/stable/oneprovider?anchor=tag/Custom-File-Metadata).

## Compiling

```bash
go build main.go
```

## Running

This script sets metadata key `s3.content.md5` for a Onedata file based on a content read from an input file. The content of the input file should be generated using a command:

```bash
# Calculates md5 sums for every file in a directory tree
find . -type f -exec /usr/bin/md5sum '{}' \;
```

resulting output should look like this:

```txt
668d3276c1cd39e41469db84bc4e309b  mnk21/01-MP/01-MP-000276#002_rew.CR2
ae5af66ddb94dadbbabf82b5008de5c7  mnk21/01-MP/01-MP-000162-32#klin.CR2
b69752bca642c6c10063a7ec70348849  mnk21/01-MP/01-Mp-000163-I-II#001.dng
```

To run the script:

```bash
./main <onedata api token> <input file>
```

For each file processed the script writes the value of `s3.content.md5` with file's path to the `log.txt` file, which is exactly the same format as an input file. By comparing them it's later possible to determine if any of the files were not processed correctly.

## Customization

At this state most of the values are hardcoded:

- the list of oneproviders addresses to query,
- the mapping of directories to Onedata spaces,
- the number of consumers (in this case we expect only one producer).
