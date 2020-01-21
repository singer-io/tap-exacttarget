# tap-exacttarget

Author: Connor McArthur (connor@fishtownanalytics.com)

This is a [Singer](http://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It:

- Generates a catalog of available data in Exacttarget
- Extracts the following resources:
  - [Campaigns](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/campaign.htm) ([source](../master/tap_exacttarget/endpoints/campaigns.py))
  - [Content Areas](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/contentarea.htm) ([source](../master/tap_exacttarget/endpoints/content_areas.py))
  - [Data Extensions](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/dataextension.htm) and their corresponding [rows](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/dataextensionobject.htm) ([source](../master/tap_exacttarget/endpoints/data_extensions.py))
  - [Emails](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/email.htm) ([source](../master/tap_exacttarget/endpoints/emails.py))
  - Events: Each of [BounceEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/bounceevent.htm), [ClickEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/clickevent.htm), [OpenEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/openevent.htm), [SentEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/sentevent.htm), [UnsubEvent](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/unsubevent.htm) go into a single `event` table ([source](../master/tap_exacttarget/endpoints/events.py))
  - [Folders](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/datafolder.htm) ([source](../master/tap_exacttarget/endpoints/folders.py))
  - [List Subscribers](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/listsubscriber.htm) ([source](../master/tap_exacttarget/endpoints/list_subscribers.py))
  - [Lists](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/list.htm) ([source](../master/tap_exacttarget/endpoints/lists.py))
  - [Sends](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/send.htm) ([source](../master/tap_exacttarget/endpoints/sends.py))
  - [Subscribers](https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/send.htm) (requires List Subscribers) ([source](../master/tap_exacttarget/endpoints/subscribers.py))

### Quick Start

1. Install

```bash
git clone git@github.com:fishtown-analytics/tap-exacttarget.git
cd tap-exacttarget
pip install .
```

2. Get credentials from Exacttarget. You'll need to:

- Create a Salesforce Marketing Cloud App
- Authenticate it to your Exacttarget account
- Get client ID and secret. Save these -- you'll need them in the next step.
- Find out if the sales force integration package is created (after 1st Aug, 2019) with only [OAuth2 support](https://help.salesforce.com/articleView?id=mc_rn_january_2019_platform_ip_enhanced_functionality_oauth2_0.htm&type=5)
- Find your tenant subdomain **{tenant-subdomin}**.login.exacttarget.com
- Obtian a refresh token following the steps [here](https://developer.salesforce.com/docs/atlas.en-us.mc-app-development.meta/mc-app-development/access-token-app.htm)

3. Create the config file.

There is a template you can use at `config.json.example`, just copy it to `config.json` in the repo root and insert your client_id, client_secret, tenant_subdomain and refresh_token.

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

Embedded FuelSDK Copyright &copy; 2019 Salesforce and Licensed under the MIT License

Copyright &copy; 2019 Stitch
