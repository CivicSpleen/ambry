// based  on https://github.com/ghiculescu/jekyll-table-of-contents
// but heavily altered
$(document).ready(function() {

  var headers = $('h1, h2, h3, h4, h5, h6').filter(function() {
      return this.id
  }) // get all headers with an ID
      
  var output = $('.toc');
      
  if (!headers.length || headers.length < 3 || !output.length)
    return;

  var get_level = function(ele) { return parseInt(ele.nodeName.replace("H", ""), 10) }
  var highest_level = headers.map(function(_, ele) { return get_level(ele) }).get().sort()[0]

  var level = get_level(headers[0])
  var this_level = 0
  var level = 0
  var html = "<ul class=\"bs-sidenav\">"
  
  headers.on('click', function() {
      window.location.hash = this.id
  }).addClass('clickable-header').each(function(_, header) {
      
    this_level = get_level(header);
    
    while (this_level > level) {// higher level than before; end parent ol
          html += "<ul class=\"nav\">"
          level ++
    }   
    
    while (this_level < level) {// higher level than before; end parent ol
          html += "</ul>"
          level --
    }   
    
    if (this_level === level) { // same level as before; same indenting
      html += "<li><a href='#" + header.id + "'>" + header.innerHTML + "</a></li>";
    } 
    
    level = this_level; // update for the next one
    
  });
  
  html += "</ul>";

  output.html(html);
});