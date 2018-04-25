---
title: DataWave Releases
permalink: /release/index.html
---
<div class="post-list">
<hr>

{% assign published = site.categories.release | where:"draft",false %}
{% assign YYYY = published[0].date | date: "%Y" %}

<h3>{{YYYY}}</h3>

{% for release in published %}

  {% assign release_year = release.date | date: "%Y" %}

  {% if release_year != YYYY %}
    {% assign YYYY = release_year %}
    <hr>
    <h3>{{ YYYY }}</h3>
  {% endif %}

  <div class="row" style="margin-top: 15px">
    <div class="col-md-1">{{ release.date | date: "%b %d" }}</div>
    <div class="col-md-10">
       <a href="{{ site.baseurl }}{{ release.url }}">{{ release.title }}</a></div>
  </div>

{% endfor %}
</div>
