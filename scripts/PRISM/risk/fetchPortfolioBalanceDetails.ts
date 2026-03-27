import "dotenv/config";
import { MongoClient } from "mongodb";
import { ethers } from "ethers";
import crypto from "node:crypto";

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════

const MONGODB_URI = process.env.MONGODB_CONNECTION_STRING!;
const MONGODB_DB = "prod";
const MONGODB_COLLECTION = "portfolio_balance_details";

const FX_BASE_URL = "https://api.falconx.io";
const FX_API_KEY = process.env.FALCONX_API_KEY!;
const FX_API_SECRET = process.env.FALCONX_API_SECRET!;
const FX_PASSPHRASE = process.env.FALCONX_PASSPHRASE!;

const ETHPLORER_BASE = "https://api.ethplorer.io";
const ETHPLORER_KEY = process.env.ETHPLORER_API_KEY || "freekey";

const HARUKO_BASE_URL = "https://sgp10.haruko.io/cefi";
const HARUKO_BEARER_TOKEN = process.env.HARUKO_BEARER_TOKEN!;

const DUNE_API_KEY = process.env.DUNE_API_KEY!;
const DUNE_BASE = "https://api.dune.com/api/v1";

// Saved Dune query IDs (created once, executed each run)
const DUNE_QUERY_LIT_PRICE = 6914770;       // LIT (Lighter) token price from prices.latest
const DUNE_QUERY_LIGHTER_NET_USDC = 6914771; // Net USDC in Lighter DEX for wallet 0xd225ea

// LIT staking: separate wallet not tracked by Ethplorer wallet list
const LIT_STAKING_QUANTITY = 1094.0;

// On-chain wallets grouped by strategy.
// harukoVenueName: if set, Haruko /api/balance is used for this wallet instead of Ethplorer
// (needed for DeFi protocol tokens that Ethplorer cannot price, e.g. Centrifuge JAAA).
const ONCHAIN_WALLETS: {
  strategy: string; deployment: string; address: string; harukoVenueName?: string;
}[] = [
  { strategy: "Tokenized RWA",              deployment: "PRISM Yield Wallet",   address: "0x889e9c6d484201394afd6bce17996a16a8bbda92" },
  { strategy: "Tokenized RWA",              deployment: "Buffer Wallet",        address: "0x10a5b1e4eb6887317c6a69f250dd740c01089fed" },
  { strategy: "Tokenized RWA",              deployment: "PRISM Funding Wallet", address: "0xb81a777a96603e69f990954b29ecf07f20669fb8" },
  { strategy: "Tokenized RWA",              deployment: "Monarq Ops Wallet",    address: "0xec3995cf2188f535a6ce1ea9da0968500bba4970" },
  // DeFi positions: use Hybrid venues (track NAV/position value) not the raw DeFi wallet venues
  // (Hybrid venues have protocol-computed USD values; DeFi wallet venues only see on-chain tokens
  //  that Haruko has market prices for, so Centrifuge JAAA / Neutrl SNUSD show as $0 there)
  { strategy: "DeFi",  deployment: "JAAA",           address: "0x8ac314d779e892c0f6818456f9e569b43d151ed4", harukoVenueName: "PRISM Monarq Operations (Centrifuge) JAAA Position" },
  { strategy: "DeFi",  deployment: "PYUSD",          address: "0x7d2e7131ba6885dc8064ce56977ce618262f88a5", harukoVenueName: "PRISM Monarq Sentora PYUSD Vault ETHEREUM" },
  { strategy: "DeFi",  deployment: "SNUSD",          address: "0xd29fda60ab08b540d38300649af706ada9da1331", harukoVenueName: "PRISM Monarq Operations (Neutrl) Position" },
  { strategy: "DeFi",  deployment: "Steakhouse AUSD",address: "0x72ac7351fa9c064b89fb8344cc920553300af6b4", harukoVenueName: "PRISM Monarq Steakhouse AUSD Position" },
  { strategy: "Overcollateralized Lending", deployment: "Maple",                address: "0x9f78d300b9b8804107930a40b09f73e7b0f85dcc" },
  { strategy: "Cash & Carry",               deployment: "Lighter (USDC & ETH)", address: "0xd225ea0888161c23f90cfd0fdc83bfa55e070f57" },
];

// ═══════════════════════════════════════════════════════════════════
// FALCONX API
// ═══════════════════════════════════════════════════════════════════

function fxAuthHeaders(method: string, path: string, body = ""): Record<string, string> {
  const timestamp = String(Date.now() / 1000);
  const prehash = timestamp + method.toUpperCase() + path + body;
  const secretBytes = Buffer.from(FX_API_SECRET, "base64");
  const signature = crypto.createHmac("sha256", secretBytes).update(prehash).digest("base64");
  return {
    "FX-ACCESS-KEY": FX_API_KEY,
    "FX-ACCESS-SIGN": signature,
    "FX-ACCESS-TIMESTAMP": timestamp,
    "FX-ACCESS-PASSPHRASE": FX_PASSPHRASE,
    "Content-Type": "application/json",
  };
}

async function fxFetch(path: string): Promise<any> {
  const headers = fxAuthHeaders("GET", path);
  const res = await fetch(FX_BASE_URL + path, { method: "GET", headers });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`FalconX ${path}: ${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

// ═══════════════════════════════════════════════════════════════════
// DUNE API — execute saved query and wait for result
// ═══════════════════════════════════════════════════════════════════

async function duneExecuteAndFetch(queryId: number): Promise<any[]> {
  // Trigger execution
  const execRes = await fetch(`${DUNE_BASE}/query/${queryId}/execute`, {
    method: "POST",
    headers: { "X-Dune-API-Key": DUNE_API_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({}),
  });
  if (!execRes.ok) throw new Error(`Dune execute ${queryId}: ${execRes.status}`);
  const { execution_id } = await execRes.json();

  // Poll for completion
  for (let attempt = 0; attempt < 30; attempt++) {
    await new Promise((r) => setTimeout(r, 2000));
    const statusRes = await fetch(`${DUNE_BASE}/execution/${execution_id}/results`, {
      headers: { "X-Dune-API-Key": DUNE_API_KEY },
    });
    if (!statusRes.ok) continue;
    const body = await statusRes.json();
    if (body.state === "QUERY_STATE_COMPLETED") return body.result?.rows ?? [];
    if (body.state === "QUERY_STATE_FAILED") throw new Error(`Dune query ${queryId} failed: ${body.error}`);
  }
  throw new Error(`Dune query ${queryId} timed out`);
}

// ═══════════════════════════════════════════════════════════════════
// HARUKO API — authoritative source for DeFi wallet valuations
// ═══════════════════════════════════════════════════════════════════

async function harukoFetch(path: string, params?: Record<string, string>): Promise<any> {
  const url = new URL(`${HARUKO_BASE_URL}${path}`);
  if (params) Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), {
    headers: { Authorization: `Bearer ${HARUKO_BEARER_TOKEN}` },
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`Haruko ${path}: ${res.status}`);
  return res.json();
}

/** Fetch USD balances for wallets that have a harukoVenueName.
 *  Returns map: lowercase_address → { totalUSD, tokens } */
async function fetchHarukoWalletValues(
  wallets: { address: string; harukoVenueName: string }[]
): Promise<Map<string, { totalUSD: number; tokens: TokenBalance[] }>> {
  const result = new Map<string, { totalUSD: number; tokens: TokenBalance[] }>();
  if (wallets.length === 0) return result;

  let venuesData: any;
  try {
    venuesData = await harukoFetch("/api/venues");
  } catch (e: any) {
    console.warn(`[Haruko/venues] ${e.message}`);
    return result;
  }

  // Build map: venue name → venue id (response is wrapped in "result")
  const nameToId = new Map<string, number>();
  const groups = venuesData.result?.groups ?? venuesData.groups ?? [];
  for (const group of groups) {
    for (const acct of group.venues ?? []) {
      if (acct.name && acct.id != null) nameToId.set(acct.name, acct.id);
    }
  }

  await Promise.all(
    wallets.map(async ({ address, harukoVenueName }) => {
      const venueId = nameToId.get(harukoVenueName);
      if (!venueId) {
        console.warn(`[Haruko] venue not found: "${harukoVenueName}"`);
        return;
      }
      try {
        const data = await harukoFetch("/api/balance", { venueAccountId: String(venueId) });
        const bals: any[] = data.result?.balances ?? data.balances ?? [];
        const tokens: TokenBalance[] = bals
          .filter((b: any) => Math.abs(b.equityUsd ?? 0) > 0.01)
          .map((b: any) => ({
            asset: b.asset ?? "?",
            contractAddress: "",
            balance: Math.abs(Number(b.equity ?? b.withdrawable ?? 0)),
            usdValue: Math.abs(Number(b.equityUsd ?? 0)),
            priceSource: "haruko" as const,
          }));
        const totalUSD = tokens.reduce((s, t) => s + t.usdValue, 0);
        result.set(address.toLowerCase(), { totalUSD, tokens });
      } catch (e: any) {
        console.warn(`[Haruko/balance/${harukoVenueName}] ${e.message}`);
      }
    })
  );
  return result;
}

// ═══════════════════════════════════════════════════════════════════
// ERC4626 ON-CHAIN PRICE FETCH
// ═══════════════════════════════════════════════════════════════════

const ERC4626_ABI = [
  "function asset() view returns (address)",
  "function decimals() view returns (uint8)",
  "function convertToAssets(uint256 shares) view returns (uint256 assets)",
  "function totalAssets() view returns (uint256)",
  "function totalSupply() view returns (uint256)",
];
const ERC20_ABI = ["function decimals() view returns (uint8)"];
// Centrifuge Liquidity Pools TrancheToken stores latestPrice as uint128
// where price = asset_amount_per_share * 10^(token_decimals) / 10^(asset_decimals)
// In practice: price / 10^tokenDecimals = USD value per share (assuming stablecoin asset)
const CENTRIFUGE_TRANCHE_ABI = [
  "function latestPrice() view returns (uint128)",
  "function decimals() view returns (uint8)",
];

/** Try to get the price of a receipt token in USD (assuming $1 underlying stablecoin).
 *
 *  Strategy 1 — Standard ERC4626: asset() + convertToAssets(1 share)
 *  Strategy 2 — Fallback ratio:   totalAssets() / totalSupply()
 *  Strategy 3 — Centrifuge TrancheToken: latestPrice() / 10^decimals
 *
 *  Returns null if no strategy produces a sensible price. */
async function fetchErc4626Price(
  tokenAddress: string,
  provider: ethers.JsonRpcProvider
): Promise<number | null> {
  const vault = new ethers.Contract(tokenAddress, ERC4626_ABI, provider);

  // ── Strategy 1: standard ERC4626 ──────────────────────────────
  try {
    const [shareDecimals, underlyingAddress] = await Promise.all([
      vault.decimals().then(Number),
      vault.asset(),
    ]);
    const underlying = new ethers.Contract(underlyingAddress, ERC20_ABI, provider);
    const underlyingDecimals: number = Number(await underlying.decimals());
    const oneShare = ethers.parseUnits("1", shareDecimals);
    const assets: bigint = await vault.convertToAssets(oneShare);
    const price = Number(assets) / 10 ** underlyingDecimals;
    if (price >= 0.9 && price <= 100) return price;
  } catch { /* fall through */ }

  // ── Strategy 2: totalAssets / totalSupply with decimal detection ─
  try {
    const [shareDecimals, totalAssets, totalSupply] = await Promise.all([
      vault.decimals().then(Number),
      vault.totalAssets(),
      vault.totalSupply(),
    ]);
    if (totalSupply === 0n) return null;

    for (const underlyingDecimals of [6, 18, 8]) {
      const price = (Number(totalAssets) / Number(totalSupply))
        * (10 ** shareDecimals / 10 ** underlyingDecimals);
      if (price >= 0.9 && price <= 100) return price;
    }
  } catch { /* fall through */ }

  // ── Strategy 3: Centrifuge TrancheToken latestPrice() ──────────
  // latestPrice is stored as: price_in_asset_units * 10^tokenDecimals / 10^assetDecimals
  // For USDC-denominated vaults (assetDecimals=6), price / 10^tokenDecimals gives USD value.
  try {
    const tranche = new ethers.Contract(tokenAddress, CENTRIFUGE_TRANCHE_ABI, provider);
    const [latestPrice, tokenDecimals] = await Promise.all([
      tranche.latestPrice(),
      tranche.decimals().then(Number),
    ]);
    // Centrifuge stores price with tokenDecimals precision (assuming 6-decimal stablecoin asset)
    const price = Number(latestPrice) / 10 ** tokenDecimals;
    if (price >= 0.9 && price <= 100) return price;
  } catch { /* fall through */ }

  return null;
}

// ═══════════════════════════════════════════════════════════════════
// ETHPLORER — ON-CHAIN WALLET BALANCES
// ═══════════════════════════════════════════════════════════════════

interface TokenBalance {
  asset: string;
  contractAddress: string;
  balance: number;
  usdValue: number;
  priceSource: "ethplorer" | "erc4626" | "dune" | "dune-flow" | "haruko";
}

async function fetchWalletBalances(
  address: string,
  provider: ethers.JsonRpcProvider,
  lighterNetUsdc: number
): Promise<{ totalUSD: number; tokens: TokenBalance[] }> {
  const res = await fetch(
    `${ETHPLORER_BASE}/getAddressInfo/${address}?apiKey=${ETHPLORER_KEY}`,
    { signal: AbortSignal.timeout(30000) }
  );
  if (!res.ok) throw new Error(`Ethplorer ${address}: ${res.status}`);
  const data = await res.json();

  const tokens: TokenBalance[] = [];
  let totalUSD = 0;
  const isLighter = address.toLowerCase() === "0xd225ea0888161c23f90cfd0fdc83bfa55e070f57";

  // ETH balance
  const ethInfo = data.ETH ?? {};
  const ethBal = Number(ethInfo.balance ?? 0);
  const ethRate = typeof ethInfo.price === "object" ? Number(ethInfo.price?.rate ?? 0) : 0;
  const ethUSD = ethBal * ethRate;
  if (Math.abs(ethUSD) >= 0.01) {
    tokens.push({ asset: "ETH", contractAddress: "", balance: ethBal, usdValue: ethUSD, priceSource: "ethplorer" });
    totalUSD += ethUSD;
  }

  // ERC-20 tokens
  for (const tok of data.tokens ?? []) {
    const info = tok.tokenInfo;
    if (!info || typeof info !== "object") continue;
    const symbol: string = info.symbol ?? "???";
    const decimals = Number(info.decimals ?? 18) || 18;
    const contractAddress: string = info.address ?? "";
    const rawBal = Number(tok.balance ?? 0);
    const balance = rawBal / 10 ** decimals;
    if (balance <= 0.0001) continue;

    const priceInfo = typeof info.price === "object" ? info.price : null;
    let rate = Number(priceInfo?.rate ?? 0) || 0;
    let priceSource: TokenBalance["priceSource"] = "ethplorer";

    // No Ethplorer price — try ERC4626 on-chain
    if (rate === 0 && contractAddress && balance > 1) {
      const erc4626Price = await fetchErc4626Price(contractAddress, provider);
      if (erc4626Price !== null) {
        rate = erc4626Price;
        priceSource = "erc4626";
      }
    }

    const usdValue = balance * rate;
    tokens.push({ asset: symbol, contractAddress, balance, usdValue, priceSource });
    totalUSD += usdValue;
  }

  // Lighter: add net USDC deployed in Lighter DEX (from Dune)
  if (isLighter && lighterNetUsdc > 0) {
    tokens.push({
      asset: "USDC (in Lighter DEX)",
      contractAddress: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
      balance: lighterNetUsdc,
      usdValue: lighterNetUsdc,
      priceSource: "dune-flow",
    });
    totalUSD += lighterNetUsdc;
  }

  return { totalUSD, tokens };
}

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

async function main() {
  console.log("═══ Portfolio Balance Details Snapshot ═══\n");

  const provider = new ethers.JsonRpcProvider(process.env.ETH_RPC_URL!);

  // ── 1. Fetch Dune data (LIT price + Lighter net USDC) in parallel ──
  console.log("[Dune] Fetching LIT price and Lighter net USDC...");
  const [litRows, lighterRows] = await Promise.all([
    duneExecuteAndFetch(DUNE_QUERY_LIT_PRICE).catch((e) => { console.warn(`[Dune/LIT] ${e.message}`); return []; }),
    duneExecuteAndFetch(DUNE_QUERY_LIGHTER_NET_USDC).catch((e) => { console.warn(`[Dune/Lighter] ${e.message}`); return []; }),
  ]);
  const litPriceUSD: number = litRows[0]?.price ?? 0;
  const lighterNetUsdc: number = Math.max(0, lighterRows[0]?.net_usdc_in_lighter ?? 0);
  console.log(`  LIT price: $${litPriceUSD.toFixed(4)}`);
  console.log(`  Lighter net USDC: $${lighterNetUsdc.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);

  // ── 2. FalconX CEX balances ────────────────────────────────────────
  let cexPositions: any[] = [];
  let cexTotalNAV = 0;

  try {
    console.log("\n[FalconX] Fetching portfolio_balance_details...");
    const balances: any[] = await fxFetch("/v1/portfolio_balance_details");
    const byVenue: Record<string, any> = {};

    for (const e of balances) {
      const netUSD = parseFloat(e.net_balance_usd ?? "0");
      const venue = e.venue ?? "unknown";
      if (!byVenue[venue]) byVenue[venue] = { netBalanceUSD: 0, spotBalanceUSD: 0, borrowedUSD: 0, openOrdersUSD: 0, positions: [] };
      byVenue[venue].netBalanceUSD    += netUSD;
      byVenue[venue].spotBalanceUSD   += parseFloat(e.spot_balance_usd ?? "0");
      byVenue[venue].borrowedUSD      += parseFloat(e.borrowed_usd ?? "0");
      byVenue[venue].openOrdersUSD    += parseFloat(e.open_orders_usd ?? "0");
      cexTotalNAV += netUSD;

      if (Math.abs(netUSD) >= 1) {
        byVenue[venue].positions.push({
          asset: e.asset,
          wallet: e.wallet ?? null,
          subaccount: e.subaccount ?? null,
          side: netUSD >= 0 ? "LONG" : "SHORT",
          quantity: parseFloat(e.net_balance ?? "0"),
          netBalanceUSD: netUSD,
          spotBalanceUSD: parseFloat(e.spot_balance_usd ?? "0"),
          borrowedUSD: parseFloat(e.borrowed_usd ?? "0"),
          openOrdersUSD: parseFloat(e.open_orders_usd ?? "0"),
          price: parseFloat(e.price ?? "0"),
        });
      }
    }

    cexPositions = Object.entries(byVenue).map(([venue, d]) => ({
      venue,
      netBalanceUSD: d.netBalanceUSD,
      spotBalanceUSD: d.spotBalanceUSD,
      borrowedUSD: d.borrowedUSD,
      openOrdersUSD: d.openOrdersUSD,
      positions: d.positions.sort((a: any, b: any) => Math.abs(b.netBalanceUSD) - Math.abs(a.netBalanceUSD)),
    })).sort((a, b) => Math.abs(b.netBalanceUSD) - Math.abs(a.netBalanceUSD));

    console.log(`  CEX NAV: $${cexTotalNAV.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);
  } catch (e: any) {
    console.error(`[FalconX] ${e.message}`);
  }

  // ── 3. FalconX derivatives ─────────────────────────────────────────
  let derivatives: any[] = [];
  let totalNetDelta = 0;

  try {
    console.log("[FalconX] Fetching position_details...");
    const positions: any[] = await fxFetch("/v1/position_details");
    for (const p of positions) {
      if (p.instrument_type !== "Futures") continue;
      const notionalUSD = Number(p.notional_usd ?? 0);
      if (Math.abs(notionalUSD) < 1) continue;
      derivatives.push({
        venue: p.venue,
        ticker: p.ticker ?? "",
        side: (p.side ?? "").toUpperCase(),
        quantity: p.quantity?.value ? parseFloat(p.quantity.value) : 0,
        notionalUSD,
      });
      totalNetDelta += notionalUSD;
    }
    derivatives.sort((a, b) => Math.abs(b.notionalUSD) - Math.abs(a.notionalUSD));
  } catch (e: any) {
    console.warn(`[FalconX/position_details] ${e.message}`);
  }

  // ── 4. On-chain wallet balances ────────────────────────────────────
  console.log("\n[On-chain] Fetching wallet balances...");

  // Pre-fetch Haruko values for DeFi wallets (more accurate than Ethplorer for protocol tokens)
  const harukoWallets = ONCHAIN_WALLETS
    .filter((w): w is typeof w & { harukoVenueName: string } => !!w.harukoVenueName)
    .map((w) => ({ address: w.address, harukoVenueName: w.harukoVenueName }));
  const harukoValues = await fetchHarukoWalletValues(harukoWallets)
    .catch((e: any) => { console.warn(`[Haruko] ${e.message}`); return new Map(); });

  const onchainResults: {
    strategy: string; deployment: string; address: string;
    totalUSD: number; tokens: TokenBalance[]; error?: string;
  }[] = [];

  for (const w of ONCHAIN_WALLETS) {
    // Use Haruko data for DeFi wallets; Ethplorer for everything else
    const harukoData = harukoValues.get(w.address.toLowerCase());
    if (harukoData) {
      onchainResults.push({ strategy: w.strategy, deployment: w.deployment, address: w.address, totalUSD: harukoData.totalUSD, tokens: harukoData.tokens });
      console.log(`  ${w.deployment}: $${harukoData.totalUSD.toLocaleString("en-US", { minimumFractionDigits: 2 })} (Haruko)`);
      continue;
    }
    try {
      const { totalUSD, tokens } = await fetchWalletBalances(w.address, provider, lighterNetUsdc);
      onchainResults.push({ strategy: w.strategy, deployment: w.deployment, address: w.address, totalUSD, tokens });
      console.log(`  ${w.deployment}: $${totalUSD.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);
      await new Promise((r) => setTimeout(r, 500)); // Ethplorer rate limit
    } catch (e: any) {
      console.error(`  ${w.deployment}: ERROR — ${e.message}`);
      onchainResults.push({ strategy: w.strategy, deployment: w.deployment, address: w.address, totalUSD: 0, tokens: [], error: e.message });
    }
  }

  // ── 5. LIT staking (separate wallet, price from Dune) ─────────────
  const litStakingUSD = LIT_STAKING_QUANTITY * litPriceUSD;
  onchainResults.push({
    strategy: "Cash & Carry",
    deployment: "LIT Staking Rewards",
    address: "",
    totalUSD: litStakingUSD,
    tokens: [{ asset: "LIT", contractAddress: "0xb59490ab09a0f526cc7305822ac65f2ab12f9723", balance: LIT_STAKING_QUANTITY, usdValue: litStakingUSD, priceSource: "dune" }],
  });
  console.log(`  LIT Staking Rewards: $${litStakingUSD.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);

  // ── 6. Build strategy breakdown ────────────────────────────────────
  const strategyMap: Record<string, { totalUSD: number; deployments: any[] }> = {};

  for (const r of onchainResults) {
    if (!strategyMap[r.strategy]) strategyMap[r.strategy] = { totalUSD: 0, deployments: [] };
    strategyMap[r.strategy].totalUSD += r.totalUSD;
    strategyMap[r.strategy].deployments.push({ deployment: r.deployment, address: r.address, totalUSD: r.totalUSD, tokens: r.tokens });
  }

  if (!strategyMap["Cash & Carry"]) strategyMap["Cash & Carry"] = { totalUSD: 0, deployments: [] };
  strategyMap["Cash & Carry"].totalUSD += cexTotalNAV;
  strategyMap["Cash & Carry"].deployments.push({ deployment: "FalconX CEX", address: "", totalUSD: cexTotalNAV, tokens: [] });

  const strategies = Object.entries(strategyMap)
    .map(([strategy, d]) => ({ strategy, totalUSD: d.totalUSD, deployments: d.deployments.sort((a, b) => b.totalUSD - a.totalUSD) }))
    .sort((a, b) => b.totalUSD - a.totalUSD);

  const onchainTotal = onchainResults.reduce((s, r) => s + r.totalUSD, 0);
  const groupEquity = onchainTotal + cexTotalNAV;

  console.log(`\n  On-chain total:   $${onchainTotal.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);
  console.log(`  CEX total:        $${cexTotalNAV.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);
  console.log(`  Group equity:     $${groupEquity.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);
  console.log(`  Net delta (deriv):$${totalNetDelta.toLocaleString("en-US", { minimumFractionDigits: 2 })}`);

  // ── 7. Build + persist document ────────────────────────────────────
  const now = new Date();
  const date = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const dateKey = date.toISOString().slice(0, 10);

  const doc = {
    snapshotId: now.toISOString(),
    date,
    dateKey,
    groupEquityUSD: groupEquity,
    onchainTotalUSD: onchainTotal,
    cexTotalNAV,
    derivativesNetDeltaUSD: totalNetDelta,
    strategies,
    cexPositions,
    derivatives,
    onchainWallets: onchainResults.map((r) => ({
      strategy: r.strategy, deployment: r.deployment, address: r.address,
      totalUSD: r.totalUSD, tokens: r.tokens, ...(r.error ? { error: r.error } : {}),
    })),
    priceSources: { litPriceUSD, lighterNetUsdcFromDune: lighterNetUsdc },
    fetchedAt: new Date(),
  };

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  try {
    const db = client.db(MONGODB_DB);
    const col = db.collection(MONGODB_COLLECTION);
    await col.createIndex({ snapshotId: 1 }, { unique: true, sparse: true });
    await col.createIndex({ dateKey: 1 });
    await col.createIndex({ date: 1 });
    const result = await col.insertOne(doc);
    console.log(`\n[${MONGODB_COLLECTION}] Inserted: ${result.insertedId} (${doc.snapshotId})`);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
