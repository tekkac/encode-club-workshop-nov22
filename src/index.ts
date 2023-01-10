import { AppDataSource } from "./data-source";
import { AppIndexer } from "./indexer";
import express, { Request, Response } from "express";
import { hexToBuffer } from "@apibara/protocol";
import { Token, Transfer } from "./entities";

const ZERO_ADDRESS = hexToBuffer("0x0000000000000000000000000000000000000000000000000000000000000000",32)

async function main() {
  await AppDataSource.initialize();

  const app = express();

  app.get("/account/:address", async (req: Request, resp: Response) => {
    const { address } = req.params;
    const owner = hexToBuffer(address, 32);
    const tokens = await AppDataSource.manager.findBy(Token, { owner });
    resp.json({
      address: address,
      tokens: tokens.map((t) => t.toJson()),
    });
  });

  app.get("/accounts", async (req: Request, resp: Response) => {
    const sender = ZERO_ADDRESS;
    const transfers = await AppDataSource.manager.findBy(Transfer, { sender });
    let owners = [];
    for (let t of transfers) {
      let owner = t.toJson().recipient;
      if (!owners.includes(owner)) {
        owners.push(owner)
      }
    } 
    resp.json({
      num_owners: owners.length,
      owners: owners
    });
  });

  app.listen(8080, () => {
    console.log("Server is running at localhost:8080");
  });
  
  await run_forever()
}

async function run_forever(){
  const indexer = new AppIndexer(
    "sea-starktest-indexer",
    "goerli.starknet.stream.apibara.com"
  );

  try {
    await indexer.run();
  } catch (e) {
    console.log(e);
    if (e.details = "Received RST_STREAM with code 0") {
      await run_forever()
    }
  }
}

main()
  .then(() => process.exit(0))
  .catch(console.error);
