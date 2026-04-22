# icms-import-agent

Orquestrador local de importação de arquivos fiscais (SPEDs + XMLs NF-e/NFC-e) para o projeto [ICMS-ST Restituição](https://github.com/allanmcz/restituicao-icmsst).

## Motivação

Clientes entregam um diretório com centenas de arquivos fiscais misturados:
- SPEDs fiscais (`.txt` — registros EFD por mês)
- NF-e de entrada (`.xml` ou `.zip` com XMLs)
- NF-e de saída (idem)
- NFC-e (idem — modelo 65, varejo)

Cada tipo vai para um pipeline diferente no backend. A classificação manual é propensa a erro e não escala.

Este agente recebe **uma pasta** como entrada, descobre o que tem, classifica cada arquivo, e dispara os uploads na ordem correta respeitando os rate limits do Supabase.

## Status

🚧 **Em construção.** Esta pasta contém:

- **`src/_ref/`** — scripts funcionais usados na primeira rajada manual (2025 + 2024). Referência para o trabalho.
- **`PROMPT_ANTIGRAVITY.md`** — prompt para o agente AI completar a implementação.
- Estrutura de pastas, `.env.example`, `package.json` prontos.

Ver `PROMPT_ANTIGRAVITY.md` para o escopo da próxima iteração.

## Uso (quando estiver pronto)

```bash
# 1. Clonar + instalar
git clone <repo-privado>
cd icms-import-agent
npm install

# 2. Configurar credenciais
cp .env.example .env
# edita .env com SUPABASE_URL, EMAIL, SENHA

# 3. Ver plano (não executa nada)
npm run dry -- /caminho/da/pasta

# 4. Executar
npm run import -- /caminho/da/pasta

# 5. Opções
node src/cli.mjs /path --year 2023         # só um ano
node src/cli.mjs /path --only sped         # só SPEDs
node src/cli.mjs /path --only xml          # só XMLs
node src/cli.mjs /path --skip-xmls-with-zero-items  # exemplo de filtro futuro
```

## Arquitetura planejada

```
src/
├── cli.mjs              # entry: parse argv, carrega .env, chama orchestrator
├── discovery.mjs        # varre pasta recursivamente, detecta arquivos
├── classify-sped.mjs    # lê registro 0000 do .txt, extrai CNPJ + período
├── classify-xml.mjs     # abre 1 XML do ZIP, detecta NF-e vs NFC-e e emitente
├── supabase.mjs         # login, wrappers de fetch/RPC, retries
├── sped.mjs             # pipeline SPED (adaptado de _ref/sped_rajada_v3.2.mjs)
├── xml.mjs              # pipeline XML (adaptado de _ref/xml_import.mjs)
└── orchestrator.mjs     # plano → execução (SPEDs primeiro, depois XMLs)
```

## Decisões de design

- **Execução local, manual.** Não é agendado. Arquivos estão no Mac do user, não cabe em edge function.
- **Credenciais por `.env`.** Nunca hardcode.
- **`--dry-run` sempre disponível.** Mostra o plano sem executar — permite auditoria.
- **Idempotência.** Reexecutar não duplica (SPED+XML têm UNIQUE constraint).
- **Observabilidade viva.** Log por arquivo (start, tempo, resultado) + relatório final.
- **Respeito aos rate limits do Supabase**: R5 (concorrência SPED), 100/h por janela, cooldown pós-504.

## Limitações conhecidas

- **Não descompacta RAR.** Se achar `.rar`, alerta e pula. Usuário precisa converter para ZIP antes.
- **Depende de `unzip` do macOS/Linux.** Usa o binário via `execSync` para extrair ZIPs (mais simples que lib JS).
- **Volume massivo pode levar horas.** NFC-e de 439 MB levou 49 min. É esperado.

## Evolução futura

- Cache de classificação entre runs (evita re-abrir ZIPs já vistos).
- `--parallel-zips N` para processar múltiplos ZIPs XML em paralelo (hoje sequencial).
- Suporte a ingestão de CSVs de apoio (alíquotas, MVA, FECOEP).
- Empacotar como binário via `pkg` ou `bun build`.
