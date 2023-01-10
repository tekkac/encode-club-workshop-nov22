import {
  credentials,
  NodeClient,
  proto,
  hexToBuffer,
  bufferToHex,
} from "@apibara/protocol";
import { Block, Transaction, TransactionReceipt } from "@apibara/starknet";
import BN from "bn.js";
import { getSelectorFromName } from "starknet/dist/utils/hash";
import { EntityManager } from "typeorm";
import { AppDataSource } from "./data-source";
import { State, Token, Transfer } from "./entities";

const SEASTARKTEST_MINT_BLOCK = 514_130;
const SEASTARKTEST_ADDRESS = hexToBuffer(
  "0x05a85cf2c715955a5d8971e01d1d98e04c31d919b6d59824efb32cc72ae90e63",
  32
);

const PROJECT_ADDRESSES = [
  "0x030f5a9fbcf76e2171e49435f4d6524411231f257a1b28f517cf52f82279c06b",
  "0x05a85cf2c715955a5d8971e01d1d98e04c31d919b6d59824efb32cc72ae90e63",
  "0x022ddbb66fabf9ae859de95c499839ff46362128908d5e3d0842368aef8beb31",
  "0x003d062b797ca97c2302bfdd0e9b687548771eda981d417faace4f6913ed8f2a",
  "0x021f433090908c2e7a6672cdbc327f49ac11bcc922611620c2c4e0d915a83382",
  "0x028c87a966e2f1166ba7fa8ae1cd89b47e13abcc676e5f7c508145751bbb7f15",
  "0x05c30f6043246a0c4e45a0316806e053e63746fba3584e1f4fc1d4e7f5300acf",
].map((addr) => hexToBuffer(addr, 32));

const TRANSFER_KEY = hexToBuffer(getSelectorFromName("Transfer"), 32);

export class AppIndexer {
  private readonly client: NodeClient;
  private readonly indexerId: string;

  constructor(indexerId: string, url: string) {
    this.indexerId = indexerId;
    this.client = new NodeClient(url, credentials.createSsl());
  }

  async run() {
    // resume from where it left the previous run
    const state = await AppDataSource.manager.findOneBy(State, {
      indexerId: this.indexerId,
    });
    let startingSequence = SEASTARKTEST_MINT_BLOCK;
    if (state) {
      startingSequence = state.sequence + 1;
    }

    const messages = this.client.streamMessages({
      startingSequence,
    });

    messages.on("data", this.handleData.bind(this));

    // keep running until the stream finishes
    return new Promise((resolve, reject) => {
      messages.on("end", resolve);
      messages.on("error", reject);
    });
  }

  async handleData(message: proto.StreamMessagesResponse__Output) {
    if (message.data) {
      if (!message.data.data.value) {
        throw new Error("received invalid data");
      }
      const block = Block.decode(message.data.data.value);
      await this.handleBlock(block);
    } else if (message.invalidate) {
      console.log(message.invalidate);
    }
  }

  async handleBlock(block: Block) {
    if (block.blockNumber % 1000 == 0) {
      console.log("Block");
      console.log(`    hash: ${bufferToHex(Buffer.from(block.blockHash.hash))}`);
      console.log(`  number: ${block.blockNumber}`);
      console.log(`    time: ${block.timestamp.toISOString()}`);
    }
    await AppDataSource.manager.transaction(async (manager) => {
      for (let receipt of block.transactionReceipts) {
        const tx = block.transactions[receipt.transactionIndex];
        await this.handleTransaction(manager, tx, receipt);
      }

      // updated indexed block
      await manager.upsert(
        State,
        { indexerId: this.indexerId, sequence: block.blockNumber },
        { conflictPaths: ["indexerId"] }
      );
    });
  }

  async handleTransaction(
    manager: EntityManager,
    tx: Transaction,
    receipt: TransactionReceipt
  ) {
    for (let event of receipt.events) {
      if (!PROJECT_ADDRESSES.some((addr) => addr.equals(event.fromAddress))) {
        continue;
      }
      if (!TRANSFER_KEY.equals(event.keys[0])) {
        continue;
      }

      const senderAddress = Buffer.from(event.data[0]);
      const recipientAddress = Buffer.from(event.data[1]);
      const tokenId = uint256FromBytes(
        Buffer.from(event.data[2]),
        Buffer.from(event.data[3])
      );

      console.log("  transfers");
      console.log(
        `    ${bufferToHex(senderAddress)} -> ${bufferToHex(recipientAddress)}`
      );
      console.log(`      ${tokenId.toString()}`);

      await manager.insert(Transfer, {
        sender: senderAddress,
        recipient: recipientAddress,
        tokenId: tokenId.toBuffer(),
      });

      await manager.upsert(
        Token,
        { id: tokenId.toBuffer(), owner: recipientAddress },
        { conflictPaths: ["id"] }
      );
    }
  }
}

function uint256FromBytes(low: Buffer, high: Buffer): BN {
  const lowB = new BN(low);
  const highB = new BN(high);
  return highB.shln(128).add(lowB);
}
