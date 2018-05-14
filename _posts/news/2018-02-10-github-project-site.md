---
title: About This Site
tags: [news]
---

This [Jekyll][jekyll] site is hosted via [GitHub Pages][github_io]. The source for the site can be viewed
[here on our repo][site_source]

{% if site.github_editme_path %}
To propose changes to the site's content, use the <a target="_blank" href="https://github.com/{{ site.github_editme_path }}{{ page.path }}" class="btn btn-default githubEditButton" role="button"><i class="fa fa-github fa-lg"></i> Edit me</a>
links wherever they appear (typically in the upper-right corner of the page) and submit a pull request
{% endif %}

[jekyll]: https://jekyllrb.com
[github_io]: https://pages.github.com
[site_source]: {{ site.repository_url }}/tree/gh-pages
