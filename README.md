# Wikidata Textifier

**Wikidata Textifier** is an API that transforms Wikidata items into compact JSON formats or textual representations for use in LLMs and GenAI applications. It resolves missing labels of properties and claim values by querying the Wikidata Action API, making it efficient and suitable for AI pipelines.

🔗 Live API: [https://wd-textify.toolforge.org/](https://wd-textify.toolforge.org/)
---

## Functionalities

- **Textifies** any Wikidata item into a readable or JSON format suitable for LLMs.
- **Resolves all labels**, including those missing when querying the Wikidata API.
- **Caches labels** for 90 days to boost performance and reduce API load.
- **Avoids SPARQL** and uses the Wikidata Action API for better efficiency and compatibility.
- **Hosted on Toolforge**: [https://wd-textify.toolforge.org/](https://wd-textify.toolforge.org/)

---

## API Usage

### `GET /`

#### Query Parameters

| Name           | Type    | Required | Description                                                                 |
|----------------|---------|----------|-----------------------------------------------------------------------------|
| `id`           | string  | Yes      | Wikidata item ID (e.g., `Q42`)                                              |
| `lang`         | string  | No       | Language code for labels (default: `en`)                                   |
| `json`         | bool    | No       | If `true`, returns JSON. If `false`, returns text representation (default: `false`) |
| `external_ids` | bool    | No       | Whether to include external IDs in the output (default: `true`)            |
