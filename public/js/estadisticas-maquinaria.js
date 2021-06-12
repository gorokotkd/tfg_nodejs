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
        success: function (res) {
            if (Chart.getChart('ventas-semanales') != null) {
                Chart.getChart('ventas-semanales').destroy();
            }
            if (Chart.getChart('ventas-mensuales') != null) {
                Chart.getChart('ventas-mensuales').destroy();
            }
            if (Chart.getChart('ventas-triMes') != null) {
                Chart.getChart('ventas-triMes').destroy();
            }

            var semana_data = {
                labels: res.semana_labels,
                datasets: [{
                    label: "Ventas del Sector",
                    data: res.semana_sector,
                    fill: false,
                    lineTension: 0.3,
                    backgroundColor: "rgba(75, 192, 192, 0.05)",
                    borderColor: "rgba(75, 192, 192, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointBorderColor: "rgba(75, 192, 192, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointHoverBorderColor: "rgba(75, 192, 192, 1)",
                    pointHitRadius: 10,
                    pointBorderWidth: 2
                },
                {
                    label: "Ventas de la empresa",
                    data: res.semana_nif,
                    fill: false,
                    borderColor: "rgb(255, 99, 132)",
                    lineTension: 0.3,
                    backgroundColor: "rgba(255, 99, 132, 0.05)",
                    borderColor: "rgba(255, 99, 132, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointBorderColor: "rgba(255, 99, 132, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointHoverBorderColor: "rgba(255, 99, 132, 1)",
                    pointHitRadius: 10
                }]
            };

            var semana_config = {
                type: "line",
                data: semana_data,
                options: char_options
            }

            var semana_ctx = document.getElementById('ventas-semanales');
            var semana_chart = new Chart(semana_ctx, semana_config);

            var mes_data = {
                labels: res.mes_labels,
                datasets: [{
                    label: "Ventas del Sector",
                    data: res.mes_sector,
                    fill: false,
                    lineTension: 0.3,
                    backgroundColor: "rgba(75, 192, 192, 0.05)",
                    borderColor: "rgba(75, 192, 192, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointBorderColor: "rgba(75, 192, 192, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointHoverBorderColor: "rgba(75, 192, 192, 1)",
                    pointHitRadius: 10,
                    pointBorderWidth: 2
                },
                {
                    label: "Ventas de la empresa",
                    data: res.mes_nif,
                    fill: false,
                    borderColor: "rgb(255, 99, 132)",
                    lineTension: 0.3,
                    backgroundColor: "rgba(255, 99, 132, 0.05)",
                    borderColor: "rgba(255, 99, 132, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointBorderColor: "rgba(255, 99, 132, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointHoverBorderColor: "rgba(255, 99, 132, 1)",
                    pointHitRadius: 10
                }]
            };

            var mes_config = {
                type: "line",
                data: mes_data,
                options: char_options
            }

            var mes_ctx = document.getElementById('ventas-mensuales');
            var mes_chart = new Chart(mes_ctx, mes_config);

            var triMes_data = {
                labels: res.triMes_labels,
                datasets: [{
                    label: "Ventas del Sector",
                    data: res.triMes_sector,
                    fill: false,
                    lineTension: 0.3,
                    backgroundColor: "rgba(75, 192, 192, 0.05)",
                    borderColor: "rgba(75, 192, 192, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointBorderColor: "rgba(75, 192, 192, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(75, 192, 192, 1)",
                    pointHoverBorderColor: "rgba(75, 192, 192, 1)",
                    pointHitRadius: 10,
                    pointBorderWidth: 2
                },
                {
                    label: "Ventas de la empresa",
                    data: res.triMes_nif,
                    fill: false,
                    borderColor: "rgb(255, 99, 132)",
                    lineTension: 0.3,
                    backgroundColor: "rgba(255, 99, 132, 0.05)",
                    borderColor: "rgba(255, 99, 132, 1)",
                    pointRadius: 3,
                    pointBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointBorderColor: "rgba(255, 99, 132, 1)",
                    pointHoverRadius: 3,
                    pointHoverBackgroundColor: "rgba(255, 99, 132, 1)",
                    pointHoverBorderColor: "rgba(255, 99, 132, 1)",
                    pointHitRadius: 10
                }]
            };

            var triMes_config = {
                type: "line",
                data: triMes_data,
                options: char_options
            }

            var triMes_ctx = document.getElementById('ventas-triMes');
            var triMes_chart = new Chart(triMes_ctx, triMes_config);

            $('#loading_gif').hide();
            $('#charts').show();

        }
    });
}