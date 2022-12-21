var svg = d3.select("svg").style("font", "12px sans-serif").call(d3.zoom().on("zoom", function () {
  svg.attr("transform", d3.event.transform)
}))
  .append("g")
  , // если убрать style  шрифт поменяется
  width = +svg.attr("width"),
  height = +svg.attr("height")
  ;

/* //arrows (part 1 of 2)
svg.append("svg:defs").selectAll("marker")
.data(["end"])      // Different link/path types can be defined here
.enter().append("svg:marker")    // This section adds in the arrows
.attr("id", String)
.attr("viewBox", "0 -5 10 10")
.attr("refX", 27)
.attr("refY", 0.5)
.attr("markerWidth", 7)
.attr("markerHeight", 7)
.attr("orient", "auto")
 .attr('fill', '#00000') // #A0A0A0 серый, #00000 черный
 .append("svg:path")
   .attr("d", "M0,-5L10,0L0,5") */




// var color = d3.scaleOrdinal(d3.schemeCategory20); // рандомная раскраска вершин графа (1 из 2)

var simulation = d3.forceSimulation()
  .force("link", d3.forceLink().id(function (d) { return d.id; }))
  .force("charge", d3.forceManyBody().strength(-800)) // мера зазреженности графа (-100 очень ужат, -2000 разрежен)
  .force("center", d3.forceCenter(1400 / 2, 800 / 2))
  ;


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
    .attr('stroke-opacity', 1)//.attr("marker-end", "url(#end)") //arrows (part 2 of 2)


  var node = svg.append("g")
    .attr("class", "nodes")
    .selectAll("g")
    .data(graph.nodes)
    .enter().append("g");
  // изменение прозрачности вершин

  var circle = node.append('circle')
    .attr('r', 10)
    .attr('stroke', 'black')
    .attr('fill', function (d, i) {
      switch (d.id.split('#')[1]) {
        case "PER":
          return "#008000"; // Green
        case "LOC":
          return "#FFD700"; // Gold
        case "ORG":
          return "#FA8072"; // Salmon
        case "MISC":
          return "#87CEEB"; // SkyBlue
      }
      return "#FFFFFF";
      // return color(i); // рандомная раскраска вершин графа (2 из 2)
    })

  var labels = node.append("text")
    .text(function (d) {
      return d.id.split('#')[0];
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
      return d.amount + " news";
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

    })
    .on("click", clicked);


  function clicked(d, i) {     // Передать данные линии и отображение текста

    if (document.getElementById("div_info")) {
      var tooltip = d3.select("#my_dataviz")
        .select("#div_info")
        .style("visibility", "hidden")
        .text(d.news[0].join('<br>'))
        ;
    } else {
      var tooltip = d3.select("#my_dataviz")
        .append("div")
        .attr('id', 'div_info')
        .style("position", "static")
        .style("visibility", "hidden")
        .attr('text-anchor', 'middle')
        .text(d.news[0].join('<br>'))
        .on("click", function () { return tooltip.style("visibility", "hidden"); })
        ;
    }

    d3.selectAll("#div_info").style("visibility", "hidden")
    tooltip.style("visibility", "visible")
      .style("top", (event.pageY))
      .style("left", (event.pageX))
      .style('width', "px")
      .style('height', "px")
      ;
    // преобразовываем текст новостей в HTML для того, что бы он переносился на основе тегов <br>
    // изначально текст в объекте d3 представлен как innerText и все теги экранируются 
    document.getElementById("div_info").innerHTML = document.getElementById("div_info").innerText

  }



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

function save_data_to_localstorage(input_id) {
  const input_val = document.getElementById(input_id).value;
  localStorage.setItem(input_id, input_val);
  console.log(input_val);
}


input_txt_1.addEventListener("change", function () {
  save_data_to_localstorage("input_txt_1");
});

input_date_1.addEventListener("change", function () {
  save_data_to_localstorage("input_date_1")
});

input_date_2.addEventListener("change", function () {
  save_data_to_localstorage("input_date_2");
});

input_num_1.addEventListener("change", function () {
  save_data_to_localstorage("input_num_1");
});

input_num_2.addEventListener("change", function () {
  save_data_to_localstorage("input_num_2");
});


function init_values() {
  if (localStorage["input_txt_1"]) {
    input_txt_1.value = localStorage["input_txt_1"];
  }

  if (localStorage["input_date_1"]) {
    input_date_1.value = localStorage["input_date_1"];
  }
  if (localStorage["input_date_2"]) {
    input_date_2.value = localStorage["input_date_2"];
  }
  if (localStorage["input_num_1"]) {
    input_num_1.value = localStorage["input_num_1"];
  }
  if (localStorage["input_num_2"]) {
    input_num_2.value = localStorage["input_num_2"];
  }
}

init_values();


// dates
var date = new Date();
var day = date.getDate();
var month = date.getMonth() + 1;
var year = date.getFullYear();
if (month < 10) month = "0" + month;
if (day < 10) day = "0" + day;
var today = year + "-" + month + "-" + day;
document.getElementById('input_date_2').value = today;

var Date_start = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toLocaleDateString('en-GB') // -14 дней (1-я цифра кол-во дней)
var words = Date_start.split('/');
var new_date_start = words[2] + "-" + words[1] + "-" + words[0]
document.getElementById('input_date_1').value = new_date_start;
