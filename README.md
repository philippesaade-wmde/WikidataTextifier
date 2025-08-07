# Wikidata Textifier

**Wikidata Textifier** is an API that transforms Wikidata items into compact format for use in LLMs and GenAI applications. It resolves missing labels of properties and claim values by querying the Wikidata Action API, making it efficient and suitable for AI pipelines.

🔗 Live API: [https://wd-textify.toolforge.org/](https://wd-textify.toolforge.org/)

---

## Functionalities

- **Textifies** any Wikidata item into a readable or JSON format suitable for LLMs.
- **Resolves all labels**, including those missing when querying the Wikidata API.
- **Caches labels** for 90 days to boost performance and reduce API load.
- **Avoids SPARQL** and uses the Wikidata Action API for better efficiency and compatibility.
- **Hosted on Toolforge**: [https://wd-textify.toolforge.org/](https://wd-textify.toolforge.org/)

---

## Formats

- **Text**: A textual representation or summary of the Wikidata item, including its label, description, aliases, and claims. Useful for helping LLMs understand what the item represents.
- **Triplet**: Outputs each triplet as a structured line, including labels and IDs, but omits descriptions and aliases. Ideal for agentic LLMs to traverse and explore Wikidata.
- **JSON**: A structured and compact representation of the full item, suitable for custom formats.

---

## API Usage

### `GET /`

#### Query Parameters

| Name           | Type    | Required | Description                                                                 |
|----------------|---------|----------|-----------------------------------------------------------------------------|
| `id`           | string  | Yes      | Wikidata item ID (e.g., `Q42`)                                              |
| `lang`         | string  | No       | Language code for labels (default: `en`)                                   |
| `format`         | string    | No       | The format of the response, either 'json', 'text', or 'triplet' (default: `json`) |
| `external_ids` | bool    | No       | Whether to include external IDs in the output (default: `true`)            |

---

## Deploy to Toolforge

1. Shell into the Toolforge system:

```bash
ssh [UNIX shell username]@login.toolforge.org
```

2. Switch to tool user account:

```bash
become wd-textify
```

3. Build from Git:

```bash
toolforge build start https://github.com/philippesaade-wmde/WikidataTextifier.git
```

4. Start the web service:

```bash
webservice buildservice start --mount all
```

5. Debugging the web service:

Read the logs:
```bash
webservice logs
```

Open the service shell:
```bash
webservice shell
```
