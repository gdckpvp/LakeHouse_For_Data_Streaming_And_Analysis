BUCKET_PATH = "s3a://test"
TABLE_PATHS = {
    "bronze_btc": "/bronze/bitcoin_stg",
    "bronze_eth": "/bronze/ethereum_stg",
    "dim_date": "/silver/dimdate",
    "dim_time": "/silver/dimtime",
    "dim_coin": "/silver/dimcoin",
    "fact": "/silver/fact",
    "dimcoin_stg": "bronze/dimcoin_stg"
}

LIST_COINS = 'bitcoin,ethereum,binance-coin,tether,solana,usd-coin,xrp,dogecoin,cardano,shiba-inu,avalanche,wrapped-bitcoin,tron,chainlink,bitcoin-cash,polkadot,near-protocol,polygon,litecoin,uniswap,unus-sed-leo,multi-collateral-dai,internet-computer,ethereum-classic,filecoin,render-token,stacks,monero,stellar,okb,crypto-com-coin,the-graph,arweave,vechain,maker,injective-protocol,theta,cosmos,fantom,thorchain,lido-dao,fetch,aave,algorand,hedera-hashgraph,flow,gala,bitcoin-sv,axie-infinity,chiliz,quant,kucoin-token,akash-network,singularitynet,neo,the-sandbox,gnosis-gno,pendle,elrond-egld,tezos,mina,ecash,gatetoken,eos,nexo,decentraland,conflux-network,dexe,nervos-network,livepeer,pancakeswap,klaytn,aioz-network,oasis-network,kava,iota,theta-fuel,synthetix-network-token,helium,wootrade,1inch,nxm,ftx-token,iotex,curve-dao-token,trueusd,trust-wallet-token,mantra-dao,wemix,xinfin-network,compound,superfarm,golem-network-tokens,raydium,ocean-protocol,rocket-pool,zcash,aragon,blox,ankr'