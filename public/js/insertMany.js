function enviarForm() {
    $('#loading_gif').show();
    $('#charts-div').hide();
    $('#qrcode').hide();
    $('#loading_gif').show();
    //$('#qrcode_label').text(res.tbai_id);
    $('#qrcode_label').hide();
    $.ajax({
        type: "get",
        url: "/insertmanyagrupadas?num=" + $('#numFacturas').val(),
        cache: false,
        success: function (res) {
            var canvas = document.getElementById('qrcode');
            var context = canvas.getContext('2d');
            context.clearRect(0, 0, canvas.width, canvas.height);

            //var url = `localhost:3000/gr?id=${encodeURIComponent(res.tbai_id)}`;
            var url = `158.227.112.237:3000/gr?id=${encodeURIComponent(res.tbai_id)}`;
            QRCode.toCanvas(canvas, url, function (err) {
                if (err) {
                    console.log(err);
                }
                $('#loading_gif').hide();
                $('#qrcode').show();
                $('#qrcode_label').text(res.tbai_id);
                $('#qrcode_label').show();
            });

            createCharts(res.stats);
        }
    });
}

function createCharts(stats) {

    var ctx = document.getElementById('insert-chart');

    if(Chart.getChart('insert-chart')!=null){
        Chart.getChart('insert-chart').destroy();
    }

    if(Chart.getChart('compress-chart') != null){
        Chart.getChart('compress-chart').destroy();
    }

    
    
    /*
    var context = ctx.getContext('2d');
    context.clearRect(0,0,ctx.width, ctx.height);
    context.restore();*/

    var labels = ["MongoDB", "Cassandra"];
    var data = {
        labels: labels,
        datasets: [{
            label: "Tiempo de Inserción de Datos en BD",
            data: [stats.insert_mongo, stats.insert_cassandra],
            backgroundColor: ["rgba(255, 99, 132, 0.9)", "rgba(54, 162, 235, 0.9)"],
            borderColor: ["rgb(255, 99, 132)", "rgb(54, 162, 235)"],
            borderWidth: 1
        }]
    };

    var config = {
        type: 'bar',
        data: data,
        options: {
            maintainAspectRatio: false,
          layout: {
            padding: {
              left: 10,
              right: 25,
              top: 25,
              bottom: 0
            }
          },
          scales: {
            xAxes: [{
              gridLines: {
                display: false,
                drawBorder: false
              },
              ticks: {
                maxTicksLimit: 6
              },
              maxBarThickness: 25,
            }],
            yAxes: [{
              ticks: {
                min: 0,
                max: 2000,
                maxTicksLimit: 2,
                padding: 10,
                // Include a dollar sign in the ticks
                callback: function(value, index, values) {
                  return number_format(value) + "ms";
                }
              },
              gridLines: {
                color: "rgb(234, 236, 244)",
                zeroLineColor: "rgb(234, 236, 244)",
                drawBorder: false,
                borderDash: [2],
                zeroLineBorderDash: [2]
              }
            }],
          },
          legend: {
            display: false
          },
          tooltips: {
            titleMarginBottom: 10,
            titleFontColor: '#6e707e',
            titleFontSize: 14,
            backgroundColor: "rgb(255,255,255)",
            bodyFontColor: "#858796",
            borderColor: '#dddfeb',
            borderWidth: 1,
            xPadding: 15,
            yPadding: 15,
            displayColors: false,
            caretPadding: 10,
            callbacks: {
              label: function(tooltipItem, chart) {
                var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + " ms";
              }
            }
          },
        }
    };
    let insert_chart = new Chart(ctx, config);


    var ctx_compress = document.getElementById('compress-chart');


    var labels_compress = ["Cassandra", "MongoDB Total"];
    labels_compress.push();
    var dataset_data = [stats.comprimir_total_cassandra, stats.comprimir_mongo.reduce((a, b) => a + b, 0)];
    var dataset_color = ["rgba(54, 162, 235, 0.9)", "rgba(255, 99, 132, 0.9)"];
    var dataset_border = ['rgb(54, 162, 235)', 'rgb(255, 99, 132)'];

    for (var i = 1; i <= stats.comprimir_mongo.length; i++) {
        labels_compress.push("MongoDB Part_" + i);
        dataset_data.push(stats.comprimir_mongo[i - 1]);
        dataset_color.push("rgba(153, 102, 255, 0.9)");
        dataset_border.push('rgb(153, 102, 255)');
    }


    var data_compres = {
        labels: labels_compress,
        datasets: [{
            label: "Tiempo de Compresión de Datos",
            data: dataset_data,
            backgroundColor: dataset_color,
            borderColor: dataset_border,
            borderWidth: 1
        }]
    };
    var config_compress = {
      type: 'bar',
      data: data_compres,
      options: {
          maintainAspectRatio: false,
        layout: {
          padding: {
            left: 10,
            right: 25,
            top: 25,
            bottom: 0
          }
        },
        scales: {
          xAxes: [{
            gridLines: {
              display: false,
              drawBorder: false
            },
            ticks: {
              maxTicksLimit: 6
            },
            maxBarThickness: 25,
          }],
          yAxes: [{
            ticks: {
              min: 0,
              max: 2000,
              maxTicksLimit: 2,
              padding: 10,
              // Include a dollar sign in the ticks
              callback: function(value, index, values) {
                return number_format(value) + "ms";
              }
            },
            gridLines: {
              color: "rgb(234, 236, 244)",
              zeroLineColor: "rgb(234, 236, 244)",
              drawBorder: false,
              borderDash: [2],
              zeroLineBorderDash: [2]
            }
          }],
        },
        legend: {
          display: false
        },
        tooltips: {
          titleMarginBottom: 10,
          titleFontColor: '#6e707e',
          titleFontSize: 14,
          backgroundColor: "rgb(255,255,255)",
          bodyFontColor: "#858796",
          borderColor: '#dddfeb',
          borderWidth: 1,
          xPadding: 15,
          yPadding: 15,
          displayColors: false,
          caretPadding: 10,
          callbacks: {
            label: function(tooltipItem, chart) {
              var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
              return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + " ms";
            }
          }
        },
      }
  };
    let compress_chart = new Chart(ctx_compress, config_compress);
    $('#charts-div').show();
}