# tap-exacttarget

Author: Connor McArthur (connor@fishtownanalytics.com)

This is a [Singer](http://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It:

- Generates a catalog of available data in Exacttarget
- Extracts the following resources:
  - [Campaigns](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/campaign.htm) ([source](../blob/master/campaigns.py))
  - [Content Areas](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/contentarea.htm) ([source](../blob/master/content_areas.py))
  - [Data Extensions](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/dataextension.htm) and their corresponding [rows](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/dataextensionobject.htm) ([source](../blob/master/data_extensions.py))
  - [Emails](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/email.htm) ([source](../blob/master/emails.py))
  - Events: Each of [BounceEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/bounceevent.htm), [ClickEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/clickevent.htm), [OpenEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/openevent.htm), [SentEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/sentevent.htm), [UnsubEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/unsubevent.htm) go into a single `event` table ([source](../blob/master/events.py))
  - [Folders](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/datafolder.htm) ([source](../blob/master/folders.py))
  - [List Subscribers](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/listsubscriber.htm) ([source](../blob/master/list_subscribers.py))
  - [Lists](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/list.htm) ([source](../blob/master/lists.py))
  - [Sends](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/send.htm) ([source](../blob/master/sends.py))
  - [Subscribers](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/send.htm) (requires List Subscribers) ([source](../blob/master/subscribers.py))

### Quick Start

1. Install

```bash
git clone git@github.com:fishtown-analytics/tap-exacttarget.git
cd tap-exacttarget
pip install .
```

2. Get credentials from Exacttarget. You'll need to:

- create a Salesforce Marketing Cloud App,
- authenticate it to your Exacttarget account, then
- get client ID and secret. Save these -- you'll need them in the next step.

3. Create the config file.

There is a template you can use at `config.json.example`, just copy it to `config.json` in the repo root and insert your client ID and secret.

4. Run the application to generate a catalog.

```bash
tap-exacttarget -c config.json --discover > catalog.json
```

5. Select the tables you'd like to replicate

Step 4 a file called `catalog.json` that specifies all the available endpoints and fields. You'll need to open the file and select the ones you'd like to replicate. See the [Singer guide on Catalog Format](https://github.com/singer-io/getting-started/blob/c3de2a10e10164689ddd6f24fee7289184682c1f/BEST_PRACTICES.md#catalog-format) for more information on how tables are selected.

6. Run it!

```bash
tap-exacttarget -c config.json --properties catalog.json
```

### Gotchas

- If you select the `subscriber` stream, you MUST select `list_subscriber` as well. `subscriber` is replicated through `list_subscriber`.

---

Embedded FuelSDK Copyright &copy; 2017 Salesforce and Licensed under the MIT License

Copyright &copy; 2017 Stitch
