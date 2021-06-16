function enviarForm() {
  $('#loading_gif').show();
  $('#error-duplicada').hide();
  $('#error-desconocido').hide();
  $('#error-archivo').hide();
  var form = $('#form')[0];
  var data = new FormData(form);
  $.ajax({
    type: 'post',
    enctype: 'multipart/form-data',
    url: '/insercion',
    data: data,
    processData: false,
    contentType: false,
    cache: false,
    success: function (res) {
      console.log(res);
      $('#loading_gif').hide();
      if (res.tbai_id == -1) {//Error desconocido
        $('#error-duplicada').hide();
        $('#error-archivo').hide();
        $('#error-desconocido').show();

      } else if (res.tbai_id == 11000) {//Error factura duplicada
        $('#error-duplicada').show();
        $('#error-desconocido').hide();

        $('#error-archivo').hide();
      }else if (res.tbai_id == -2){
        $('#error-archivo').show();
        $('#error-duplicada').hide();
        
        $('#error-desconocido').hide();
      } else {
        var canvas = document.getElementById('qrcode');
        //var url = `localhost:3000/gr?id=${encodeURIComponent(res.tbai_id)}`;
        var url = `158.227.112.237:3000/gr?id=${encodeURIComponent(res.tbai_id)}`;
        QRCode.toCanvas(canvas, url, function (err) {
          if (err) {
            console.log(err);
          }

          $('#qrcode_label').text(res.tbai_id);
          $('#qrcode_label').show();
        });

        //createCharts(res.stats);
      }


    }

  });
}