# Scratch: prose and fenced code containing "any"

Reserved flag characters are free-form: any key class may write them, and
any reader must ignore the ones it does not recognise.

The same detector-shaped text inside a fenced block in a markdown file:

```ts
let x: any = 1;
const xs: any[] = [];
const y = JSON.parse(input) as any;
```
