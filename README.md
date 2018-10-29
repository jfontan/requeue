# Requeue repositories with missing siva files

This tool checks for repositories downloaded with [borges](https://github.com/src-d/borges) that have missing siva files.

You need to create a list of available siva files, one per line. This file can be the full path or a list of hashes. For example:

```
1ae2043ec8f7f14d5719829fdc2b45e679d7568f
162d2456291181010e9ae9ebf536a5711db7ca43
f42e7fcff32e28d17e56f1946dfe2e7af565b0b6
acadb5e3257b75fb7c7d5fdaac1e2fc648c411a7
/this/path/5b/5b2b3a70df208eb4c025f60e3183df9dbfeaaf47
/another/path/69645c314aec4050712b9ef1cbe19281d9f7ccfa.siva
```

If a repository has a reference that is not in that list and the state is `fetched` a new job is created to download it. To find these repos it queries all `repository` - `init commit` pair:

```sql
select repository_id, init from repository_references group by repository_id, init
```

You can specify database and queue parameters:

```
Usage:
  requeue [OPTIONS] list

Application Options:
      --database=   database connection string (default: postgres://testing:testing@localhost/testing?sslmode=disable)
      --queue=      rabbitmq connection string (default: amqp://localhost:5672)
      --queue-name= queue name to send new jobs (default: borges)
      --dry         do not send jobs or modify database

Help Options:
  -h, --help        Show this help message

Arguments:
  list:             file name with the list of found hashes
```
