// Script de import XML direto via RPC process_single_xml
// Uso: node xml_import.mjs <caminho_do_zip>
// Extrai ZIP, parseia NFe/NFC-e, monta payload e chama process_single_xml em paralelo
// com retry em 429/5xx. Respeita rate limit.
import { readFileSync, mkdirSync, readdirSync, statSync, rmSync, existsSync } from "node:fs";
import { join, basename } from "node:path";
import { execSync } from "node:child_process";

const MODULES_DIR = "/Users/allan/000-PROJETOS/000-RESTITUICAO_ICMSST/node_modules";

// fast-xml-parser vem do node_modules do projeto
const { XMLParser } = await import(join(MODULES_DIR, "fast-xml-parser/src/fxp.js")).catch(() =>
  import(join(MODULES_DIR, "fast-xml-parser/src/fxp.js"))
);

const SUPABASE_URL = "https://ohslwbcswmdlxxgotinq.supabase.co";
const ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9oc2x3YmNzd21kbHh4Z290aW5xIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA0ODQ1OTAsImV4cCI6MjA3NjA2MDU5MH0.qL68NxcKiIsAhYhYAKoCN6V7YFrk4PwceXFauMAIl4I";
const EMAIL = "allanmcz@gmail.com";
const PASSWORD = "Amma@2804";
const ESTABELECIMENTO_ID = "dcc229cf-edf3-4893-a979-d49c73c52e93"; // OLIVEIRA
const CNPJ_ESTAB = "17616272000113"; // só dígitos

const CONCURRENCY = 3;
const RETRY_AFTER_429_MS = 15_000;
const RETRY_AFTER_5XX_MS = 30_000;
const MAX_ATTEMPTS = 8;

const zipPath = process.argv[2];
if (!zipPath) {
  console.error("uso: node xml_import.mjs <zip_path>");
  process.exit(1);
}

if (!existsSync(zipPath)) {
  console.error(`arquivo não existe: ${zipPath}`);
  process.exit(1);
}

async function login() {
  const r = await fetch(`${SUPABASE_URL}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: { "Content-Type": "application/json", apikey: ANON_KEY },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  if (!r.ok) throw new Error("login falhou: " + (await r.text()));
  const data = await r.json();
  return { jwt: data.access_token, userId: data.user.id };
}

// Extrai ZIP para pasta tmp
function extractZip(zipPath) {
  const tmp = `/tmp/xml_import_${Date.now()}`;
  mkdirSync(tmp, { recursive: true });
  execSync(`unzip -q -o "${zipPath}" -d "${tmp}"`);
  return tmp;
}

function findXmls(dir) {
  const out = [];
  const walk = (d) => {
    for (const entry of readdirSync(d)) {
      const full = join(d, entry);
      const st = statSync(full);
      if (st.isDirectory()) walk(full);
      else if (entry.toLowerCase().endsWith(".xml")) out.push(full);
    }
  };
  walk(dir);
  return out;
}

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  parseAttributeValue: false,
  parseTagValue: false,
  trimValues: true,
  allowBooleanAttributes: true,
});

const num = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = parseFloat(String(v).replace(",", "."));
  return isNaN(n) ? null : n;
};

const int = (v) => {
  if (v === undefined || v === null || v === "") return null;
  const n = parseInt(String(v), 10);
  return isNaN(n) ? null : n;
};

const str = (v) => (v === undefined || v === null ? null : String(v));

// Extrai chave_acesso do atributo Id="NFe..." do infNFe
function getChaveFromInfNFe(infNFe) {
  const id = infNFe?.["@_Id"];
  if (!id) return null;
  const match = id.match(/(\d{44})/);
  return match ? match[1] : null;
}

// Consolida valores ICMS de diferentes CSTs em um único objeto
function consolidaICMS(icms) {
  if (!icms) return {};
  // icms tem a key ICMSXX (00, 10, 20, 30, 40, 51, 60, 70, 90) ou ICMSSN101, etc
  const inner = Object.values(icms)[0] || {};
  return {
    cst_icms: str(inner.CST || inner.CSOSN),
    bc_icms: num(inner.vBC),
    aliq_icms: num(inner.pICMS),
    valor_icms: num(inner.vICMS),
    origem: str(inner.orig),
    bc_st: num(inner.vBCST),
    aliq_st: num(inner.pICMSST),
    valor_icms_st: num(inner.vICMSST),
    p_mva_st: num(inner.pMVAST),
    bc_st_ret: num(inner.vBCSTRet),
    valor_st_ret: num(inner.vICMSSTRet),
    aliq_st_ret: num(inner.pST),
  };
}

function parseXmlToPayload(xmlContent, userId) {
  const parsed = parser.parse(xmlContent);
  // NFe pode estar dentro de nfeProc (com protocolo) ou direto
  const nfe = parsed.nfeProc?.NFe || parsed.NFe;
  if (!nfe) return null;
  const infNFe = nfe.infNFe;
  if (!infNFe) return null;

  const chave = getChaveFromInfNFe(infNFe);
  if (!chave || chave.length !== 44) return null;

  const ide = infNFe.ide || {};
  const emit = infNFe.emit || {};
  const dest = infNFe.dest || {};
  const total = infNFe.total?.ICMSTot || {};

  const emitCnpj = (str(emit.CNPJ) || "").replace(/\D/g, "");
  const destCnpj = (str(dest.CNPJ) || str(dest.CPF) || "").replace(/\D/g, "");

  // tipo_operacao: baseado no emit CNPJ vs cnpj_estab
  const tipoOperacao = emitCnpj === CNPJ_ESTAB ? "saida" : "entrada";

  // itens: <det nItem="1">, <det nItem="2">, ...
  const detRaw = infNFe.det;
  const detArr = Array.isArray(detRaw) ? detRaw : detRaw ? [detRaw] : [];
  const itens = detArr.map((det) => {
    const nItem = int(det["@_nItem"]);
    const prod = det.prod || {};
    const imposto = det.imposto || {};
    const icmsConsol = consolidaICMS(imposto.ICMS);
    const ipi = imposto.IPI?.IPITrib || imposto.IPI?.IPINT || {};
    const pis = imposto.PIS?.PISAliq || imposto.PIS?.PISNT || imposto.PIS?.PISOutr || {};
    const cofins = imposto.COFINS?.COFINSAliq || imposto.COFINS?.COFINSNT || imposto.COFINS?.COFINSOutr || {};
    return {
      numero_item: nItem,
      codigo_produto: str(prod.cProd),
      codigo_barras: str(prod.cEAN === "SEM GTIN" ? null : prod.cEAN),
      descricao: str(prod.xProd) || "SEM DESCRIÇÃO",
      ncm: str(prod.NCM),
      cest: str(prod.CEST),
      cfop: str(prod.CFOP),
      unidade: str(prod.uCom),
      quantidade: num(prod.qCom),
      valor_unitario: num(prod.vUnCom),
      valor_total: num(prod.vProd),
      valor_desconto: num(prod.vDesc),
      ...icmsConsol,
      cst_ipi: str(ipi.CST),
      bc_ipi: num(ipi.vBC),
      aliq_ipi: num(ipi.pIPI),
      valor_ipi: num(ipi.vIPI),
      cst_pis: str(pis.CST),
      bc_pis: num(pis.vBC),
      aliq_pis: num(pis.pPIS),
      valor_pis: num(pis.vPIS),
      cst_cofins: str(cofins.CST),
      bc_cofins: num(cofins.vBC),
      aliq_cofins: num(cofins.pCOFINS),
      valor_cofins: num(cofins.vCOFINS),
    };
  });

  return {
    user_id: userId,
    estabelecimento_id: ESTABELECIMENTO_ID,
    chave_acesso: chave,
    numero_nf: int(ide.nNF),
    serie: int(ide.serie),
    data_emissao: str(ide.dhEmi || ide.dEmi),
    valor_total_nf: num(total.vNF),
    valor_produtos: num(total.vProd),
    valor_desconto: num(total.vDesc),
    valor_frete: num(total.vFrete),
    emit_cnpj: emitCnpj,
    emit_nome: str(emit.xNome),
    dest_cnpj: destCnpj,
    dest_nome: str(dest.xNome),
    bc_icms: num(total.vBC),
    valor_icms: num(total.vICMS),
    bc_icms_st: num(total.vBCST),
    valor_icms_st: num(total.vST),
    valor_ipi: num(total.vIPI),
    valor_pis: num(total.vPIS),
    valor_cofins: num(total.vCOFINS),
    bc_st_ret: num(total.vBCSTRet),
    valor_st_ret: num(total.vSTRet),
    valor_fcp: num(total.vFCP),
    valor_fcp_st: num(total.vFCPST),
    valor_fcp_st_ret: num(total.vFCPSTRet),
    tipo_operacao: tipoOperacao,
    origem: "xml_import",
    status: "autorizada",
    _itens: itens,
  };
}

async function callRpc(jwt, payload) {
  const r = await fetch(`${SUPABASE_URL}/rest/v1/rpc/process_single_xml`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${jwt}`,
      apikey: ANON_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ p_nfe: payload }),
  });
  return { status: r.status, body: await r.text() };
}

// ============================================================
// MAIN
// ============================================================
const t0 = Date.now();
console.log(`=== XML Import via process_single_xml ===`);
console.log(`Zip: ${zipPath}`);
console.log(`Tamanho: ${(statSync(zipPath).size / 1024 / 1024).toFixed(2)} MB\n`);

console.log("[1/5] Login…");
const { jwt, userId } = await login();
console.log(`    user_id: ${userId}\n`);

console.log("[2/5] Extraindo ZIP…");
const extractDir = extractZip(zipPath);
console.log(`    extraído em ${extractDir}\n`);

console.log("[3/5] Listando XMLs…");
const xmlFiles = findXmls(extractDir);
console.log(`    ${xmlFiles.length} XMLs encontrados\n`);

console.log("[4/5] Processando…");
const stats = { inserted: 0, duplicates: 0, errors: 0, parseErrors: 0, retry429: 0, retry5xx: 0 };
const queue = [...xmlFiles];

async function worker(id) {
  while (queue.length > 0) {
    const file = queue.shift();
    if (!file) return;
    let payload;
    try {
      const content = readFileSync(file, "utf8");
      payload = parseXmlToPayload(content, userId);
      if (!payload) {
        stats.parseErrors++;
        continue;
      }
    } catch (err) {
      stats.parseErrors++;
      continue;
    }
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      const res = await callRpc(jwt, payload);
      if (res.status === 200) {
        try {
          const result = JSON.parse(res.body);
          if (result.duplicate) stats.duplicates++;
          else if (result.inserted) stats.inserted++;
          else stats.errors++;
        } catch {
          stats.errors++;
        }
        break;
      }
      if (res.status === 429) {
        stats.retry429++;
        await new Promise((r) => setTimeout(r, RETRY_AFTER_429_MS));
        continue;
      }
      if (res.status >= 500 || res.status === 0) {
        stats.retry5xx++;
        await new Promise((r) => setTimeout(r, RETRY_AFTER_5XX_MS));
        continue;
      }
      // 4xx não-429 = erro definitivo
      stats.errors++;
      if (stats.errors < 3) {
        console.log(`   [w${id}] erro definitivo ${res.status} em ${basename(file)}: ${res.body.substring(0, 150)}`);
      }
      break;
    }
  }
}

// Monitor
const totalXmls = xmlFiles.length;
const monitor = setInterval(() => {
  const done = stats.inserted + stats.duplicates + stats.errors + stats.parseErrors;
  const pct = ((done / totalXmls) * 100).toFixed(1);
  const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
  console.log(
    `   t+${elapsed}s  ${done}/${totalXmls} (${pct}%)  inseridos=${stats.inserted}  dup=${stats.duplicates}  erros=${stats.errors}+${stats.parseErrors}pe  retry=${stats.retry429}/${stats.retry5xx}`
  );
}, 10_000);

await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i + 1)));
clearInterval(monitor);

console.log("\n[5/5] Cleanup…");
rmSync(extractDir, { recursive: true, force: true });

const elapsed = ((Date.now() - t0) / 1000).toFixed(0);
console.log(`\n=== CONCLUÍDO em ${elapsed}s ===`);
console.log(`  Inseridos:    ${stats.inserted}`);
console.log(`  Duplicados:   ${stats.duplicates}`);
console.log(`  Erros:        ${stats.errors}`);
console.log(`  Parse errors: ${stats.parseErrors}`);
console.log(`  Retry 429:    ${stats.retry429}`);
console.log(`  Retry 5xx:    ${stats.retry5xx}`);
console.log(`  Total:        ${stats.inserted + stats.duplicates + stats.errors + stats.parseErrors}/${totalXmls}`);
