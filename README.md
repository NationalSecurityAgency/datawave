# To work with this site locally...

## Prerequisites

- Ruby: <https://rvm.io>
- Jekyll: <https://jekyllrb.com/docs/quickstart/>
- Site Source: <https://github.com/nationalsecurityagency/datawave>

## Performing site updates

- See in-line documentation in [_config.yml](_config.yml)
- See [scripts/publish-new-releases](scripts/publish-new-releases) to automate publishing of 'Project News' updates for new DataWave releases 

## Build and run the site

```bash
 # Clone datawave and pull/checkout only the gh-pages branch...
 
 $ git clone --single-branch \
   --branch gh-pages \
   https://github.com/NationalSecurityAgency/datawave.git \
   datawave-gh-pages
   ...
 $ cd datawave-gh-pages
  
 # Build and run the site using the preview server with overridden baseurl (see baseurl configuration
 # notes in _config.yml for more info), and auto-sync site changes via '--watch' option ...
  
 $ bundle update # Optional
 $ bundle exec jekyll serve --baseurl '' --watch
 
 # By default, JEKYLL_ENV=development (see details below)
 
 # Now browse to http://localhost:4000/
 
```

## Development Mode vs Production Mode

When you build/run the site locally, you'll automatically be in development mode, i.e., **JEKYLL_ENV=development**.
So, if you want to build the production site, then you must set **JEKYLL_ENV=production**, as is the case on GitHub.

Currently, the only difference between dev and prod mode is that dev mode will enable display of *TODO*, *WIP*,
and other dev-related *tags* and *includes* throughout the site.

To build and run locally in production mode...

```bash
JEKYLL_ENV=production bundle exec jekyll serve --baseurl '' --watch

```

Thus, internally, you can add dev-only elements and behaviors to the site as follows...

```
{% if jekyll.environment != 'production' %}
   I'm in development! 
{% endif %}

```
