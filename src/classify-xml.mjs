import { XMLParser } from "fast-xml-parser";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  parseAttributeValue: false,
  parseTagValue: false,
  trimValues: true,
  // Para classificação apenas, não precisamos de tudo, mas se deixarmos assim fica rápido e seguro
});

/**
 * Faz o parsing superficial de um XML para extrair metadados e tipo.
 * @param {string} xmlContent Conteúdo do arquivo XML (texto puro)
 * @param {Array<{cnpj: string}>} estabelecimentosConhecidos Lista dos seus clientes
 * @returns {Object|null} Retorna { tipo, ano, emitCnpj, mod } ou null se for inválido
 */
export function classifyXml(xmlContent, estabelecimentosConhecidos = []) {
  try {
    const parsed = parser.parse(xmlContent);

    // O NFe pode vir direto como root ou "embrulhado" no protocolo <nfeProc>
    const nfe = parsed?.nfeProc?.NFe || parsed?.NFe;
    if (!nfe || !nfe.infNFe) {
      return null;
    }

    const ide = nfe.infNFe.ide || {};
    const emit = nfe.infNFe.emit || {};

    const emitCnpj = typeof emit.CNPJ === "string" ? emit.CNPJ.replace(/\D/g, "") : (typeof emit.CNPJ === "number" ? String(emit.CNPJ) : "");
    const mod = String(ide.mod);
    const dataEmissao = String(ide.dhEmi || ide.dEmi || "");
    const ano = dataEmissao && dataEmissao.length >= 4 ? dataEmissao.substring(0, 4) : null;

    // Se é NFC-e
    if (mod === "65") {
      return { tipo: "nfce-saida", ano, emitCnpj, mod };
    }

    // Se é NF-e (55), verificamos se a emissão foi pelo estabelecimento conhecido (saída) 
    // ou por um terceiro contra o estabelecimento (entrada). No discovery, a gente vai bater esse emitCnpj.
    // Como a classificação base só diz o tipo se não souber pra qual CNPJ foi, 
    // vamos deixar a checagem complementar pro level superior se não passarem os estabelecimentos.
    
    let tipo = "nfe-entrada"; // default

    if (estabelecimentosConhecidos.length > 0) {
      if (estabelecimentosConhecidos.some(e => e.cnpj === emitCnpj)) {
        tipo = "nfe-saida";
      }
    }

    return { tipo, ano, emitCnpj, mod };
  } catch (err) {
    return null;
  }
}
