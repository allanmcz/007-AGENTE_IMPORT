import { discover } from "../src/discovery.mjs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const FIXTURES_DIR = join(__dirname, "fixtures");

async function runTests() {
  console.log("Iniciando testes de discovery...");

  const estabelecimentos = [
    { cnpj: "45312456000112", razao_social: "EMPRESA TESTE", id: "uuid-1234" }
  ];

  const result = await discover(FIXTURES_DIR, estabelecimentos);

  let errors = 0;

  const assertEqual = (name, actual, expected) => {
    if (actual !== expected) {
       console.error(`❌ [FALHOU] ${name} | Esperado: ${expected}, Obtido: ${actual}`);
       errors++;
    } else {
       console.log(`✅ [OK]     ${name}`);
    }
  };

  // Avaliar SPED
  assertEqual("Quantidade de SPEDs", result.speds.length, 1);
  if (result.speds.length === 1) {
    assertEqual("SPED CNPJ extraido corretamente", result.speds[0].cnpj, "45312456000112");
    assertEqual("SPED Data Inicial extraida", result.speds[0].periodoInicio, "01012023");
  }

  // Avaliar XMLs soltos (são 2: sample-nfe, sample-nfce)
  assertEqual("Quantidade de XMLs soltos", result.xmlFiles.length, 2);
  
  const nfe = result.xmlFiles.find(x => x.name === "sample-nfe.xml");
  const nfce = result.xmlFiles.find(x => x.name === "sample-nfce.xml");

  if (nfe) {
    assertEqual("NFe classificada como nfe-saida (pois emitCnpj = estabCnpj)", nfe.tipo, "nfe-saida");
    assertEqual("NFe extraiu o ano corretamente", nfe.year, "2023");
  } else {
    console.error("❌ [FALHOU] NFe não encontrada nos XMLs soltos");
    errors++;
  }

  if (nfce) {
    assertEqual("NFCe classificada corretamente", nfce.tipo, "nfce-saida");
    assertEqual("NFCe extraiu o ano", nfce.year, "2023");
  } else {
    console.error("❌ [FALHOU] NFCe não encontrada nos XMLs soltos");
    errors++;
  }

  // Avaliar ZIP
  assertEqual("Quantidade de ZIPs", result.xmlZips.length, 1);
  if (result.xmlZips.length === 1) {
    const zip = result.xmlZips[0];
    assertEqual("ZIP contém primeiro XML com emitCnpj coerente", zip.cnpjEstab, "45312456000112");
  }

  // Finalização
  if (errors === 0) {
     console.log(`\n🎉 Todos os testes passaram! Seu discovery está mapeando corretamente.`);
     process.exit(0);
  } else {
     console.log(`\n❗️ Falha em ${errors} testes.`);
     process.exit(1);
  }
}

runTests();
