var svg = d3.select("svg"),
  width = +svg.attr("width"),
  height = +svg.attr("height");

svg.append("svg:defs").selectAll("marker")
  .data(["end"])      // Different link/path types can be defined here
  /*.enter().append("svg:marker")    // This section adds in the arrows : Стрелки (часть 1 из 2)
    .attr("id", String)
    .attr("viewBox", "0 -5 10 10")
    .attr("refX", 27)
    .attr("refY", 0.5)
    .attr("markerWidth", 7)
    .attr("markerHeight", 7)
    .attr("orient", "auto")
    .attr('fill', '#A0A0A0') // #A0A0A0 серый, #00000 черный */
  .append("svg:path")
  .attr("d", "M0,-5L10,0L0,5")




var color = d3.scaleOrdinal(d3.schemeCategory20);

var simulation = d3.forceSimulation()
  .force("link", d3.forceLink().id(function (d) { return d.id; }))
  .force("charge", d3.forceManyBody().strength(-800)) // мера зазреженности графа (-100 очень ужат, -2000 разрежен)
  .force("center", d3.forceCenter(width / 2, height / 2));


d3.json("/data", function (error, graph) {

  if (error) throw error;

  var line = svg.append("g")
    .attr("class", "links")
    .selectAll("line")
    .data(graph.links)
    .enter()


  var link = line
    .append("line")
    .attr('fill', 'black')
    .attr('stroke', '#A0A0A0')
    .attr("stroke-width", 1)
  //.attr('stroke-opacity', 1).attr("marker-end", "url(#end)") // Стрелки (часть 2 из 2)


  var node = svg.append("g")
    .attr("class", "nodes")
    .selectAll("g")
    .data(graph.nodes)
    .enter().append("g");
  // изменение прозрачности вершин

  var circle = node.append('circle')
    .attr('r', 10)
    .attr('stroke', 'black')
    .attr('fill', function (d, i) { return color(i); })

  var labels = node.append("text")
    .text(function (d) {
      return d.id;
    })
    .attr('x', 12)
    .attr('y', 3);
  // Create a drag handler and append it to the node object instead
  var drag_handler = d3.drag()
    .on("start", dragstarted)
    .on("drag", dragged)
    .on("end", dragended);

  drag_handler(node);

  var linelabels = line
    .append('text')
    .attr('text-anchor', 'middle')
    .attr("x", 30)
    .attr("y", 30)
    .text(function (d) {
      return d.name;
    })
    .attr('opacity', 0.2)
    .on('mouseover', function (d, i) {
      d3.select(this).transition()
        .duration('50')
        .attr('opacity', '.0.99');

    })
    .on('mouseout', function (d, i) {
      d3.select(this).transition()
        .duration('50')
        .attr('opacity', '0.2');

    });




  simulation
    .nodes(graph.nodes)
    .on("tick", ticked);

  simulation.force("link")
    .links(graph.links);


  function ticked() {
    link
      .attr("x1", function (d) { return d.source.x; })
      .attr("y1", function (d) { return d.source.y; })
      .attr("x2", function (d) { return d.target.x; })
      .attr("y2", function (d) { return d.target.y; });

    node
      .attr("transform", function (d) {
        return "translate(" + d.x + "," + d.y + ")";
      })
    linelabels
      .attr("x", function (d) { return (d.source.x + d.target.x) / 2; })
      .attr("y", function (d) { return (d.source.y + d.target.y) / 2; })

  }
});

function dragstarted(d) {
  if (!d3.event.active) simulation.alphaTarget(0.3).restart();
  d.fx = d.x;
  d.fy = d.y;
}

function dragged(d) {
  d.fx = d3.event.x;
  d.fy = d3.event.y;
}

function dragended(d) {
  if (!d3.event.active) simulation.alphaTarget(0);
  d.fx = null;
  d.fy = null;
}