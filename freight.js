const express = require('express')
const router = express.Router()
const { toFixed } = require('./utils/javaScript')
const { ObjectId } = require('mongodb')
const { lte } = require('lodash')

router.get('/', async (req, res) => {
  try {
    if (!req.body.seller_id) throw 'seller_id require'

    if (req.body.destination.value.length > 8)
      throw { message: 'CEP de destino inválido', error_code: 2 }

    let db = req.mongoConnection
    let publishColl = db.collection('publish')
    let sellerColl = db.collection('seller')
    let publish = await publishColl.findOne({ publishId: req.body.items[0].id })
    let seller = await sellerColl.findOne({ _id: publish.sellerId })

    if (!publish) throw `PUblish ${req.body.items[0].id} not found on hub`

    let freightColl = db.collection('freight')

    let freights

    //Calcular frete para cliente com tabela fixa
    freights = await freightColl
      .find({
        sellerId: publish.sellerId,
        zipStart: { $lte: parseInt(req.body.destination.value) },
        zipEnd: { $gte: parseInt(req.body.destination.value) }
      })
      .toArray()

    //Procurar tabela de frete compartilhada
    if (!freights.length)
      freights = await freightColl
        .find({
          ufOrigin: seller.state,
          zipStart: { $lte: parseInt(req.body.destination.value) },
          zipEnd: { $gte: parseInt(req.body.destination.value) }
        })
        .toArray()

    if (!freights.length)
      throw 'Produto sem cobertura de envio para o CEP destino.'

    let quotations = freights.map(freight => {
      let value = freight.values
        .sort(function (x, y) {
          return x.weight - y.weight
        })
        .find(
          f =>
            f.weight >=
            req.body.items[0].dimensions.weight / req.body.items[0].quantity
        )

      let price = value
        ? value.price
        : Math.max.apply(
            null,
            freight.values.map(m => m.price)
          )
      return {
        price: toFixed(price * req.body.items[0].quantity, 2),
        handling_time: 0,
        shipping_time: Math.round(freight.estimated),
        promise: Math.round(freight.estimated),
        service: 99
      }
    })

    let retorno = {
      destinations: [req.body.destination.value.toString()],
      packages: [
        {
          dimensions: {
            height: Math.round(req.body.items[0].dimensions.height),
            width: Math.round(req.body.items[0].dimensions.width),
            length: Math.round(req.body.items[0].dimensions.length),
            weight: Math.round(req.body.items[0].dimensions.weight)
          },
          items: [
            {
              id: req.body.items[0].id,
              variation_id: req.body.items[0].variation_id,
              quantity: req.body.items[0].quantity,
              dimensions: {
                height: Math.round(req.body.items[0].dimensions.height),
                width: Math.round(req.body.items[0].dimensions.width),
                length: Math.round(req.body.items[0].dimensions.length),
                weight: Math.round(req.body.items[0].dimensions.weight)
              }
            }
          ],
          quotations
        }
      ]
    }

    let callbackLogColl = db.collection('callbackLog')
    await callbackLogColl.insertOne({
      req: req.body,
      res: retorno,
      createdAt: new Date()
    })

    res.set('Cache-Control', 'no-store')
    res.status(200).json(retorno)
  } catch (err) {
    let { message, error_code, http } = err
    res.status(http ? http : 500).json({
      message: message
        ? message
        : typeof err == 'string'
        ? err
        : 'Erro interno',
      error_code: error_code ? error_code : 1
    })
  }
})

router.get('/tabelimportfreight', async (req, res) => {
  try {
    let db = req.mongoConnection
    let freightColl = db.collection('freight')
    let list = req.body

    if (list.length > 250) throw 'Limit body array 500!'

    let consoleLog = []

    for (let freight of list) {
      try {
        let {
          name,
          sellerId,
          zipStart,
          zipEnd,
          weight,
          service,
          price,
          estimated
        } = freight

        if (!name) throw 'Name required !'
        if (!sellerId) throw 'sellerId required !'
        if (!zipStart) throw 'zipStart required !'
        if (!zipEnd) throw 'zipEnd required !'
        if (!weight) throw 'weight required !'
        if (!service) throw 'service required !'
        if (!Number.isInteger(price)) throw 'price required !'
        if (!estimated) throw 'estimated required !'

        let data = {
          service: freight.service,
          estimated: freight.estimated,
          zipStart: parseInt(freight.zipStart),
          zipEnd: parseInt(freight.zipEnd),
          values: [{ price: freight.price, weight: freight.weight }]
        }

        await freightColl.updateOne(
          {
            name: freight.name,
            sellerId: new ObjectId(freight.sellerId),
            service: freight.service,
            estimated: freight.estimated
          },
          {
            $set: {
              service: data.service,
              estimated: data.estimated,
              zipStart: data.zipStart,
              zipEnd: data.zipEnd
            },
            $push: { values: { $each: data.values } }
          },
          { upsert: true }
        )
      } catch (error) {
        consoleLog.push({ sucesso: false, erro: error, ...freight })
      }
    }
    res.status(200).json(consoleLog)
  } catch (error) {
    res.status(200).json(error)
  }
})

module.exports = app => app.use('/v1/freight', router)
