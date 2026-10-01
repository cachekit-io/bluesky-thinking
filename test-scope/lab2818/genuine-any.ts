export function parse(input: string): number {
  let x: any = 1;
  const xs: any[] = [];
  const y = JSON.parse(input) as any;
  xs.push(y);
  return x + xs.length;
}
