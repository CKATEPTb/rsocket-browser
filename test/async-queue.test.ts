/**
 * AsyncQueue behavioral and high-volume ordering tests.
 */
import { describe, expect, it } from "vitest";
import { AsyncQueue } from "@/async/index.js";

describe("AsyncQueue", () => {
  it("preserves queued undefined values", async () => {
    const queue = new AsyncQueue<void>();

    queue.push();

    await expect(queue.next()).resolves.toEqual({
      done: false,
      value: undefined
    });
  });

  it("drains large buffered queues in insertion order", async () => {
    const queue = new AsyncQueue<number>();

    for (let index = 0; index < 5_000; index += 1) {
      queue.push(index);
    }

    for (let index = 0; index < 5_000; index += 1) {
      await expect(queue.next()).resolves.toEqual({
        done: false,
        value: index
      });
    }
  });

  it("drops buffered values after consumer return", async () => {
    const queue = new AsyncQueue<number>();

    queue.push(1);
    queue.push(2);

    await expect(queue.return()).resolves.toEqual({
      done: true,
      value: undefined
    });
    await expect(queue.next()).resolves.toEqual({
      done: true,
      value: undefined
    });
  });

  it("drains buffered values before reporting a later error", async () => {
    const queue = new AsyncQueue<number>();
    const error = new Error("boom");

    queue.push(1);
    queue.push(2);
    queue.error(error);

    await expect(queue.next()).resolves.toEqual({
      done: false,
      value: 1
    });
    await expect(queue.next()).resolves.toEqual({
      done: false,
      value: 2
    });
    await expect(queue.next()).rejects.toBe(error);
  });

  it("rejects pending consumers when the queue fails without buffered values", async () => {
    const queue = new AsyncQueue<number>();
    const pending = queue.next();
    const error = new Error("boom");

    queue.error(error);

    await expect(pending).rejects.toBe(error);
    await expect(queue.next()).rejects.toBe(error);
  });

  it("resolves many pending consumers in insertion order", async () => {
    const queue = new AsyncQueue<number>();
    const pending: Array<Promise<IteratorResult<number>>> = [];

    for (let index = 0; index < 5_000; index += 1) {
      pending.push(queue.next());
    }

    for (let index = 0; index < 5_000; index += 1) {
      queue.push(index);
    }

    const results = await Promise.all(pending);
    expect(results.map((result) => result.value)).toEqual(
      Array.from({ length: 5_000 }, (_value, index) => index)
    );
  });
});
