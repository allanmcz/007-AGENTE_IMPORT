# Prompt para o agente AI — implementação do `icms-import-agent`

Copie e cole o bloco abaixo no Antigravity (ou equivalente) com este repositório aberto.

---

## Contexto

Este é um projeto Node.js (ESM, `type=module`) para orquestrar a importação de arquivos fiscais brasileiros (SPEDs + XMLs NF-e/NFC-e) para um SaaS Supabase externo. Ele é um **utilitário CLI local**, executado manualmente pelo admin quando precisa importar o histórico de um cliente.

O projeto SaaS de destino está em `/Users/allan/000-PROJETOS/000-RESTITUICAO_ICMSST` (não precisa editar nada lá — este agente só **consome** a API dele).

### O que já existe neste repo

- **Estrutura de pastas** criada (`src/`, `src/_ref/`, `test/fixtures/`).
- **`package.json`** com `fast-xml-parser` e `dotenv` como deps.
- **`.env.example`** com todas as variáveis.
- **`.gitignore`** para não subir credenciais / cache.
- **`src/_ref/`** contém 3 scripts **já funcionais e testados** com carga real (60 SPEDs + 225.000 XMLs em produção):
  - `sped_rajada_v3.2.mjs` — faz upload sequencial de SPEDs para edge function `process-sped-async`, com retry em 429/504, deadline de 10min por arquivo, requeue automático. **Credenciais e CNPJ estão hardcoded** — tem que parametrizar.
  - `xml_import.mjs` — recebe 1 zip como argumento, extrai com `unzip`, parseia cada XML com `fast-xml-parser`, chama RPC `process_single_xml` em paralelo (3 workers) respeitando rate-limit. **CNPJ e UUID do estabelecimento hardcoded** — tem que parametrizar.
  - `sped_validate.mjs` — validação pós-importação (E100/E110/E200/E210 por SPED).

### O que falta implementar

Um **orquestrador CLI** que recebe um caminho de pasta e faz tudo sozinho:

```bash
node src/cli.mjs /Users/allan/003_ALMIR_FERRAGENS/HOME_CENTER --dry-run
node src/cli.mjs /Users/allan/003_ALMIR_FERRAGENS/HOME_CENTER
node src/cli.mjs /path --year 2023
node src/cli.mjs /path --only sped
node src/cli.mjs /path --only xml
```

---

## Tarefas

### 1. `src/supabase.mjs` — cliente unificado

- Função `login()` que retorna `{ jwt, userId }` usando email/senha do `.env` via REST `/auth/v1/token?grant_type=password`.
- Função `rpc(name, params, { jwt, anonKey })` que chama `/rest/v1/rpc/<name>` e retorna `{ status, body }`.
- Função `invokeFn(fnName, { method, body, headers, jwt, anonKey })` que chama `/functions/v1/<fnName>`.
- Função `query(table, queryString, { jwt, anonKey, prefer })` para `SELECT`/`HEAD` via REST.
- Função `listEstabelecimentos({ jwt, anonKey })` que retorna `[{ id, cnpj, razao_social }]` (usado para matching por CNPJ).

### 2. `src/discovery.mjs` — varredura da pasta

Função `discover(rootPath)` retorna um objeto tipado como:

```typescript
{
  speds: Array<{ path, name, cnpj, periodoInicio, periodoFim, size }>,
  xmlZips: Array<{ path, name, size, tipo: 'nfe-entrada'|'nfe-saida'|'nfce-saida', cnpjEstab, year }>,
  xmlFiles: Array<{ path, name, size, tipo, cnpjEstab }>,  // XMLs soltos (raro)
  rarFiles: Array<{ path, name, size }>,                    // ignorados, apenas listar
  unknown: Array<{ path, reason }>
}
```

Heurísticas:
- **SPED**: arquivo `.txt` contendo `|0000|` na primeira ou segunda linha. Extrai `cnpj` (campo 8 do 0000 pós-split por `|`), `periodoInicio` (campo 4), `periodoFim` (campo 5).
- **XML solto**: `.xml` com `<NFe>` ou `<nfeProc>` no início. Reaproveita lógica de `src/_ref/xml_import.mjs` para classificar.
- **ZIP**: extrai apenas 1 XML (`unzip -p <zip> <first-xml>`) para classificar. Lê `mod` (modelo: 55=NF-e, 65=NFC-e) e `emit/CNPJ`. Se `emit/CNPJ` ∈ estabelecimentos conhecidos → `saida`. Senão → `entrada`.
- **RAR**: só lista + alerta (não tenta extrair).
- **Year**: extrai do caminho (subpasta `2024/`, `2025/` etc) ou do nome do arquivo ou do primeiro XML dentro.

### 3. `src/classify-xml.mjs` — helper para classificação de XML

Isolar a lógica de parsing mínimo (só para detectar tipo, não fazer upload) — usa `fast-xml-parser` com config mínima para rapidez.

### 4. `src/sped.mjs` — pipeline SPED

Adaptar `_ref/sped_rajada_v3.2.mjs`:
- Remover credenciais/paths hardcoded — receber via parâmetros.
- Exportar `runSpedBatch(speds, { jwt, userId, supabaseUrl, anonKey, config })` onde `speds` é array de `{ path, name, cnpj, periodoInicio, periodoFim }`.
- **Não precisa UUID do estabelecimento** — o backend `process-sped-async` cria/atualiza automaticamente baseado no registro `0000` do SPED.
- Retornar `{ uploaded, failed, retried429, retried504, jobs: [...] }` para relatório.

### 5. `src/xml.mjs` — pipeline XML

Adaptar `_ref/xml_import.mjs`:
- Remover hardcoded CNPJ, UUID, credenciais.
- Exportar `runXmlZip(zipPath, { estabelecimentoId, cnpjEstab, jwt, userId, supabaseUrl, anonKey, config })`.
- Retornar `{ inserted, duplicates, errors, parseErrors, retry429, retry5xx }`.
- Para XML solto (não-ZIP), exportar `runXmlFile(xmlPath, { … })`.

### 6. `src/orchestrator.mjs` — plano de execução

Função `buildPlan(discovery, { year, only, estabelecimentos })`:
- Filtra por ano e tipo se solicitado.
- Agrupa por estabelecimento (via CNPJ).
- Ordem de execução:
  1. SPEDs por estabelecimento, cronológico ascendente (primeiro o mais antigo — necessário pois C100 referencia 0150, 0200 etc, e XMLs saída dependem de SPEDs).
  2. XMLs de entrada por ano.
  3. XMLs de saída por ano.
  4. NFC-e por ano.
- Retorna `Array<{ tipo, arquivos, estabelecimentoCnpj }>` pronto para executar.

Função `executePlan(plan, context)`:
- Para cada item do plano, chama o pipeline apropriado.
- Agrega métricas.
- Log linha a linha com timestamp.
- Relatório final consolidado.

### 7. `src/cli.mjs` — entry point

- Parse argv: `<caminho>`, `--dry-run`, `--year=2024`, `--only=sped|xml`.
- Carrega `.env` via `dotenv`.
- Chama `discover()`, lista resultado.
- Constrói plano.
- Se `--dry-run`: imprime plano formatado e sai com 0.
- Senão: chama `login()`, `listEstabelecimentos()`, `executePlan()`.
- Erro: código 1, mensagem clara.

---

## Critérios de aceite

### Funcionais

- [ ] `npm install` não instala nada além do listado.
- [ ] `npm run dry -- /caminho/com/dados-reais` imprime um plano correto.
- [ ] `npm run import -- /caminho/com/dados-reais` importa tudo sem intervenção manual.
- [ ] Arquivos já importados previamente são **detectados como duplicatas** e não re-inseridos (RPC `process_single_xml` e `process-sped-async` já tratam isso via `ON CONFLICT DO NOTHING`).
- [ ] Filtros `--year` e `--only` funcionam.
- [ ] Erros de rede (429/504/5xx) são tratados com retry + backoff.
- [ ] RARs são listados e ignorados com warning.

### Não-funcionais

- [ ] Código em ES Modules puro, sem TypeScript.
- [ ] Zero dependência além de `fast-xml-parser` e `dotenv`.
- [ ] Usa `unzip` do sistema via `execSync` (não adicionar lib de ZIP em JS).
- [ ] Sem `process.env` direto no código de negócio — tudo via `config` passada por parâmetro.
- [ ] Cada função pura recebe todas as dependências explicitamente (facilita teste).
- [ ] Tratamento de erro: nenhum `throw` implícito — sempre try/catch com mensagem contextual.
- [ ] Cleanup: remove pastas tmp (`/tmp/xml_import_*`) no final de cada ZIP.

### Observabilidade

- [ ] Prefix nos logs com tipo: `[SPED]`, `[XML]`, `[DISCOVER]`, `[PLAN]`.
- [ ] Progresso a cada 10s (inseridos / total, ETA estimado).
- [ ] Relatório final com: tempo total, contagens por tipo, CNPJs tocados, erros.

### Testes (mínimos)

- [ ] `test/fixtures/sample-sped.txt` — 1 SPED pequeno (1 mês, ~20 registros) com CNPJ de teste.
- [ ] `test/fixtures/sample-nfe.xml` — 1 NF-e simples de entrada.
- [ ] `test/fixtures/sample-nfce.xml` — 1 NFC-e simples.
- [ ] `test/fixtures/sample.zip` — ZIP contendo os XMLs acima.
- [ ] `test/discovery.test.mjs` — roda `discover()` em `test/fixtures/` e valida classificação.
- [ ] Script: `npm test` que roda o discovery e imprime resultado.

**Não é necessário mockar o Supabase** — testes unitários cobrem só discovery/classify. Upload real é testado manualmente.

---

## Referências úteis

- **Scripts originais em `src/_ref/`** mostram toda a lógica de baixo nível (retry, backoff, parsing XML para payload da RPC). **Reaproveite extensivamente** — só falta generalizar.
- **Estrutura de XML NFe**: raiz `<nfeProc>` (com protocolo) ou `<NFe>`, dentro `<infNFe Id="NFe<44-digits>">`, `<ide>` (dhEmi, mod, tpNF, nNF, serie), `<emit>` (CNPJ, xNome), `<dest>`, `<det nItem="...">` (prod, imposto.ICMS, IPI, PIS, COFINS), `<total><ICMSTot>` (vNF, vProd, vICMS, vST, ...).
- **Estrutura SPED EFD**: arquivo texto pipe-delimited. Primeiro registro é `|0000|...|<CNPJ>|...|<CIE>|...|`. Campo 4 = DT_INI (AAAAMMDD), campo 5 = DT_FIN, campo 8 = CNPJ.
- **RPC `process_single_xml`**: assinatura `(p_nfe jsonb, p_log_entry jsonb DEFAULT null)`. Ver shape exato em `src/_ref/xml_import.mjs` (função `parseXmlToPayload`).
- **Edge function `process-sped-async`**: multipart POST com `file`, `original_size`, `compressed_size`. Trata rate limit 100/h + R5 (3 concorrentes). Retorna `{ success, job_id, sped_id, chunks }`.
- **Lista de estabelecimentos já cadastrados**: `GET /rest/v1/estabelecimentos?select=id,cnpj,razao_social`.

---

## Sequência sugerida de implementação

1. **`supabase.mjs`** primeiro — todo o resto depende dele.
2. **`discovery.mjs`** e **`classify-xml.mjs`** — testar com `test/fixtures/` antes de integrar.
3. **`cli.mjs`** com apenas `--dry-run` — roda `discover()`, imprime, sai. Permite validação visual do plano.
4. **`sped.mjs`** — adapta de `_ref`, parametriza.
5. **`xml.mjs`** — adapta de `_ref`, parametriza.
6. **`orchestrator.mjs`** — cola tudo.
7. **Smoke test** com pasta real (1 SPED + 1 ZIP pequeno) antes de rodar em produção.

---

## Definition of Done

Um admin não-técnico consegue clonar o repo, copiar `.env`, preencher 3 variáveis (URL, EMAIL, PASSWORD), rodar `npm run import -- /pasta/do/cliente`, ir tomar um café, e voltar com todo o histórico fiscal do cliente no banco — sem tocar em SQL, sem editar código, sem interpretar logs.
