# kafka-web

Simple web service that expose kafka messages as pages with pagination token.

## Example


```
$ curl localhost:8080/messages/topic-4  | jq .

{
  "messages": [
    {
      "key": "a",
      "value": "1"
    },
    {
      "key": "b",
      "value": "2"
    }
  ],
  "nextToken": "eyJ2YWx1ZSI6eyIwIjoxfX0="
}
```

```
$ curl localhost:8080/messages/topic-4?nextToken=eyJ2YWx1ZSI6eyIwIjoxfX0= | jq .
{
  "messages": [
    {
      "key": "c",
      "value": "3"
    }
  ],
  "nextToken": "eyJ2YWx1ZSI6eyIwIjoyfX0="
}

```
