'use strict'

var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var FacturaSchema = Schema({
    nif : {
        type: Schema.Types.String,
        required: true
    },
    fechaInicio: {
        type: Date,
        required: true
    },
    fechaFin: {
        type: Date,
        required: true
    },
    idents : {
        type: Schema.Types.Array,
        required : true
    },
    agrupacion : {
        type: Schema.Types.String,
        required : true
    }
});

module.exports = mongoose.model('facturas_agrupada', FacturaSchema);