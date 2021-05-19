'use strict'

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var FacturaSchema = Schema({
    _id : {
        type: Schema.Types.String,
        required: true
    },
    NIF : {
        type: Schema.Types.String,
        required : true
    },
    FechaExpedicionFactura : {
        type: Schema.Types.Date,
        required : true
    },
    HoraExpedicionFactura : {
        type: Schema.Types.Date,
        required : true
    },
    ImporteTotalFactura : {
        type : Schema.Types.Number,
        required: true
    },
    SerieFactura : {
        type: Schema.Types.String,
        required : true
    },
    NumFactura : {
        type: Schema.Types.String,
        required : true
    },
    Descripcion :{
        type: Schema.Types.String,
        required : true
    },
    FacturaComprimida : {
        required : true,
        type : Schema.Types.String
    },
    Status: {
        required : true,
        type : Schema.Types.String
    }
});

module.exports = mongoose.model('Factura', FacturaSchema);