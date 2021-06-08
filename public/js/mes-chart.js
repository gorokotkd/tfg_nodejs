var ctx = document.getElementById("mes-chart");
var myLineChart = new Chart(ctx, {
  type: 'line',
  data: {
    labels: ["2021-03-01","2021-03-02","2021-03-03","2021-03-04","2021-03-05","2021-03-06","2021-03-07","2021-03-08","2021-03-09","2021-03-10","2021-03-11","2021-03-12","2021-03-13","2021-03-14","2021-03-15","2021-03-16","2021-03-17","2021-03-18","2021-03-19","2021-03-20","2021-03-21","2021-03-22","2021-03-23","2021-03-24","2021-03-25","2021-03-26","2021-03-27","2021-03-28"],
    datasets: [{
      label: "Ingresos Medios",
      lineTension: 0.3,
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
      data: [0,0,0,23.235697674418603,21.099661016949153,20.408253012048203,23.59202247191011,0,0,0,13.83078947368421,22.950056818181817,20.39365714285714,26.18931506849315,0,0,0,21.673804347826092,22.78146341463415,20.339411764705872,18.091264367816095,0,0,0,24.059166666666666,17.90327586206896,19.319166666666668,27.810344827586203],
    }],
  },
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
            return number_format(value) + "€";
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
          return datasetLabel + ': ' + number_format(tooltipItem.yLabel) + "€";
        }
      }
    }
  }
});