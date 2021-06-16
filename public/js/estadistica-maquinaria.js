const char_options = {
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
        time: {
          unit: 'date'
        },
        gridLines: {
          display: false,
          drawBorder: false
        },
        ticks: {
          maxTicksLimit: 7
        }
      }],
      yAxes: [{
        id: 'A',
        position: 'left',
        ticks: {
          maxTicksLimit: 5,
          padding: 10,
          // Include a dollar sign in the ticks
          callback: function(value, index, values) {
            return number_format(value) + '€';
          }
        },
        gridLines: {
          color: "rgb(234, 236, 244)",
          zeroLineColor: "rgb(234, 236, 244)",
          drawBorder: false,
          borderDash: [2],
          zeroLineBorderDash: [2]
        }
      },
      {
        id: 'B',
        position: 'right',
        type: 'linear',
        ticks : {
          min: 0,
          max:500
        }
        
      }],
    },
    legend: {
      display: false
    },
    tooltips: {
      backgroundColor: "rgb(255,255,255)",
      bodyFontColor: "#858796",
      titleMarginBottom: 10,
      titleFontColor: '#6e707e',
      titleFontSize: 14,
      borderColor: '#dddfeb',
      borderWidth: 1,
      xPadding: 15,
      yPadding: 15,
      displayColors: false,
      intersect: false,
      mode: 'index',
      caretPadding: 10,
      callbacks: {
        label: function(tooltipItem, chart) {
          var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
          return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + " €";
        }
      }
    }
  };


function calcStatistics() {
    $('#charts').hide();
    $('#loading_gif').show();
    let nif = $('#nif').val();

    $.ajax({
        type: "get",
        url: "/showstatistics?sector=maquinaria&nif=" + nif,
        cache: false,
        async: false,
        error: function(xhr, status, error){
          $('#loading-gif').hide();
          $('#error').html("Error: " + xhr.responseText);
          $('#error').show();
        },
        success: function (res) {
            if (Chart.getChart('exportaciones-ventas') != null) {
                Chart.getChart('exportaciones-ventas').destroy();
            }

            var data = {
                labels: res.labels,
                //labels: ["GB", "DE", "PT", "No exportan"],
                datasets: [{
                    type: 'bar',
                    label: "Exportaciones del Sector",
                    data: res.exportaciones_sector,
                    //data: [4, 1, 2 , 3],
                    backgroundColor: "rgba(75, 192, 192, 0.5)",
                    borderColor: "rgba(75, 192, 192, 1)",
                    //yAxisID: 'A'
                },
                {
                    label: "Exportaciones de la empresa",
                    type: 'bar',
                    data: res.exportaciones_nif,
                    //data: [3, 3,1 , 2],
                    backgroundColor: "rgba(255, 99, 132, 0.5)",
                    borderColor: "rgba(255, 99, 132, 1)",
                    //yAxisID: 'A'
                },
                {
                  label: "Ingresos del Sector",
                  type: 'line',
                  //data :[123, 116, 204, 100],
                  data: res.ingresos_sector,
                  yAxisID: 'B',
                  backgroundColor: "rgba(78, 115, 223, 0.05)",
                  borderColor: "rgba(78, 115, 223, 1)",
                  pointRadius: 3,
                  pointBackgroundColor: "rgba(78, 115, 223, 1)",
                  pointBorderColor: "rgba(78, 115, 223, 1)",
                  pointHoverRadius: 3,
                  pointHoverBackgroundColor: "rgba(78, 115, 223, 1)",
                  pointHoverBorderColor: "rgba(78, 115, 223, 1)",
                  pointHitRadius: 10,
                  pointBorderWidth: 2,
                },
                {
                  label: "Ingresos de la Empresa",
                  type: 'line',
                  //data :[150, 137, 328, 450],
                  data: res.ingresos_nif,
                  yAxisID: 'B',
                  backgroundColor: "rgba(78, 115, 223, 0.05)",
                  borderColor: "rgba(23, 166, 115, 1)",
                  pointRadius: 3,
                  pointBackgroundColor: "rgba(23, 166, 115, 1)",
                  pointBorderColor: "rgba(23, 166, 115, 1)",
                  pointHoverRadius: 3,
                  pointHoverBackgroundColor: "rgba(23, 166, 115, 1)",
                  pointHoverBorderColor: "rgba(23, 166, 115, 1)",
                  pointHitRadius: 10,
                  pointBorderWidth: 2,
                }
              ]
            };

            var config = {
                type: "line",
                data: data,
                options: char_options
            }

            var ctx = document.getElementById('exportaciones-ventas');
            var chart = new Chart(ctx, config);


            $('#loading_gif').hide();
            $('#charts').show();

        }
    });
}