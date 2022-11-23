const {
  postShippingInvoice,
  getShippingLabelMeli,
  getShippingById,
  getInvoiceMeli,
  getXmlMeli,
  getLinkStorage
} = require('../http/mercadoLivre')
const { ObjectId } = require('mongodb')
const { sleep, toFixed } = require('../util/javaScript')
const { uploadFileS3 } = require('../util/storage')
const { calcFreight } = require('./sigepWeb')
const axios = require('axios')

const meliOrderToDigigrowOrder = async (
  meliOrder,
  queue,
  sellerId,
  platformId,
  db
) => {
  let items = []

  let freight =
    meliOrder.shippingInfo && meliOrder.shippingInfo.shipping_option
      ? meliOrder.shippingInfo.shipping_option.list_cost
      : 0

  let freightClient = toFixed(
    meliOrder.shippingInfo && meliOrder.shippingInfo.shipping_option
      ? meliOrder.shippingInfo.shipping_option.cost
      : 0,
    2
  )

  let freightSeller = toFixed(
    meliOrder.shippingInfo && meliOrder.shippingInfo.shipping_option
      ? meliOrder.shippingInfo.shipping_option.list_cost - freightClient
      : 0,
    2
  )

  let payments = meliOrder.payments.map(m => {
    return {
      paymentId: m.id,
      method: m.payment_method_id,
      status: m.status,
      installments: m.installments,
      value: toFixed(parseFloat(m.transaction_amount + m.shipping_cost), 2)
    }
  })

  let freightMode = []
  let sellerColl = db.collection('seller')
  let { cep } = await sellerColl.findOne({ _id: new ObjectId(sellerId) })

  if (meliOrder.shippingInfo.mode == 'me1' && freightClient == 0) {
    for (let items of meliOrder.shippingInfo.shipping_items) {
      let dimensionsSplit = items.dimensions.replace(',', 'x').split('x')
      let height = dimensionsSplit[0]
      let width = dimensionsSplit[1]
      let length = dimensionsSplit[2]
      let weight = dimensionsSplit[3]

      let consult = {
        seller_id: meliOrder.seller.id,
        items: [
          {
            id: items.id,
            price: 80,
            quantity: items.quantity,
            dimensions: {
              height,
              width,
              length,
              weight
            }
          }
        ],
        destination: {
          type: 'zipcode',
          value: meliOrder.shippingInfo.receiver_address.zip_code
        },
        origin: {
          type: 'zipcode',
          value: cep
        }
      }

      let configAxios = {
        method: 'get',
        timeout: 20000,
        url: 'http://api.digigrow.com.br:2540/v1/freight',
        headers: {
          'Content-Type': 'application/json'
          // Authorization: token
        },
        data: consult
      }

      let ret = await axios(configAxios)

      freightMode.push(ret)
    }

    if (freightMode.length > 0) {
      freightSeller += freightMode[0].data.packages[0].quotations[0].price
    }
  }

  let skuColl = db.collection('sku')
  let skus = await skuColl
    .find({
      sku: {
        $in: meliOrder.order_items.map(m =>
          m.item.seller_sku ? m.item.seller_sku : m.item.seller_custom_field
        )
      }
    })
    .toArray()

  let sellerContractId = await db
    .collection('seller')
    .findOne({ _id: sellerId })
  let contractFee = await db
    .collection('contract')
    .findOne({ _id: sellerContractId.contractId })

  if (!contractFee)
    throw `Contrato não encontrado para o seller ${sellerContractId.document}`

  let ret = []

  skus.map(m => {
    if (m && m.kit) {
      m.kit.map(mm => ret.push(mm.sku))
    }
  })

  let skusKit = await skuColl.find({ sku: { $in: ret } }).toArray()

  for (let meliItem of meliOrder.order_items) {
    let skuFilter = meliItem.item.seller_sku
      ? meliItem.item.seller_sku
      : meliItem.item.seller_custom_field
    let skuItem = skus.find(f => f.sku == skuFilter)

    if (
      skuItem &&
      skuItem.kit &&
      Array.isArray(skuItem.kit) &&
      skuItem.kit.length > 0
    ) {
      let totalPriceKit = 0
      for (let skuKit of skusKit.filter(f =>
        skuItem.kit.map(m => m.sku == f.sku)
      )) {
        totalPriceKit =
          totalPriceKit +
          skuKit.price * skuItem.kit.find(f => f.sku == skuKit.sku).quantity
      }

      for (let structKit of skuItem.kit) {
        let sku = skusKit.find(f => f.sku == structKit.sku)

        let weightSkuKit = (sku.price * structKit.quantity) / totalPriceKit
        let priceItem = meliItem.unit_price * weightSkuKit

        items.push({
          publishId: meliItem.item ? meliItem.item.id : '',
          sku: structKit.sku,
          title: sku.title,
          amount: meliItem.quantity * structKit.quantity, //calcular multiplicar
          unit: (priceItem / meliItem.quantity) * structKit.quantity,
          price: priceItem * meliItem.quantity, //calcular peso de acordo com kit/item
          discount: 0,
          gross: priceItem * meliItem.quantity,
          total: priceItem * meliItem.quantity,
          listingType: meliItem.listing_type_id,
          saleFee: meliItem.sale_fee * meliItem.quantity * weightSkuKit,
          skuKit: skuItem.sku,
          titleKit:
            meliItem.item.variation_attributes &&
            meliItem.item.variation_attributes.length
              ? meliItem.item.title +
                ' - ' +
                meliItem.item.variation_attributes[0].value_name
              : meliItem.item.title
        })
      }
    } else {
      items.push({
        publishId: meliItem.item ? meliItem.item.id : '',
        sku: meliItem.item.seller_sku
          ? meliItem.item.seller_sku
          : meliItem.item.seller_custom_field,
        title:
          meliItem.item.variation_attributes &&
          meliItem.item.variation_attributes.length
            ? meliItem.item.title +
              ' - ' +
              meliItem.item.variation_attributes[0].value_name
            : meliItem.item.title,
        amount: meliItem.quantity,
        unit: meliItem.unit_price,
        price: meliItem.unit_price * meliItem.quantity,
        discount: 0,
        gross: meliItem.unit_price * meliItem.quantity,
        total: meliItem.unit_price * meliItem.quantity,
        listingType: meliItem.listing_type_id,
        saleFee: meliItem.sale_fee * meliItem.quantity
      })
    }
  }

  let shipping = {}

  if (meliOrder.shippingInfo) {
    shipping = {
      shippingId: meliOrder.shippingInfo.id,
      mode: meliOrder.shippingInfo.mode,
      status:
        meliOrder.shippingInfo.mode == 'me2'
          ? meliOrder.shippingInfo.status
          : null,
      estimateDeliveryDate: meliOrder.shippingInfo.shipping_option
        .estimated_delivery_time
        ? meliOrder.shippingInfo.shipping_option.estimated_delivery_time.date
        : null,
      city: meliOrder.shippingInfo.receiver_address.city.name,
      state: meliOrder.shippingInfo.receiver_address.state.id.split('-')[1],
      country: meliOrder.shippingInfo.receiver_address.country.id,
      street: meliOrder.shippingInfo.receiver_address.street_name,
      neighborhood: meliOrder.shippingInfo.receiver_address.neighborhood.name,
      number: meliOrder.shippingInfo.receiver_address.street_number,
      comment: meliOrder.shippingInfo.receiver_address.comment,
      address: meliOrder.shippingInfo.receiver_address.address_line,
      zipCode: meliOrder.shippingInfo.receiver_address.zip_code,
      trackingNumber: meliOrder.shippingInfo.tracking_number,
      trackingMethod: `${meliOrder.shippingInfo.mode} - ${meliOrder.shippingInfo.shipping_option.name}`,
      fulfillment: meliOrder.shippingInfo.logistic_type == 'fulfillment'
    }
  }

  let grossMeli = items.reduce((n, { gross }) => n + gross, 0)
  let sale_fee = items.reduce((n, { saleFee }) => n + saleFee, 0)

  let grossDigi = grossMeli
  if (contractFee.freightClientFee) grossDigi = grossDigi + freightClient
  if (contractFee.freightSellerFee) grossDigi = grossDigi + freightSeller
  if (!contractFee.addDigiFee) contractFee.addDigiFee = 0

  let digiFee = toFixed(
    grossDigi * contractFee.saleFee + contractFee.addDigiFee,
    2
  )
  let receivement = grossMeli - sale_fee - freightSeller - digiFee

  if (receivement < 0) receivement = Number(receivement.toFixed(2))
  if (receivement > 0) receivement = toFixed(receivement, 2)

  return {
    externalId: meliOrder.id,
    packId: meliOrder.pack_id,
    createdAt: new Date(),
    updatedAt: new Date(),
    marketPlaceId: queue.marketPlaceId,
    sellerId,
    platformId,
    userId: queue.userId,
    saleDate: new Date(meliOrder.date_created),
    dateClosed: new Date(meliOrder.date_closed),
    status: meliOrder.status,
    buyer: {
      buyerId: meliOrder.buyer.id,
      name: `${
        meliOrder.billingInfo &&
        meliOrder.billingInfo.additional_info.find(f => f.type == 'FIRST_NAME')
          ? meliOrder.billingInfo.additional_info.find(
              f => f.type == 'FIRST_NAME'
            ).value
          : meliOrder.buyer.first_name
      } ${
        meliOrder.billingInfo &&
        meliOrder.billingInfo.additional_info.find(f => f.type == 'LAST_NAME')
          ? meliOrder.billingInfo.additional_info.find(
              f => f.type == 'LAST_NAME'
            ).value
          : meliOrder.buyer.last_name
      }`,
      email: meliOrder.buyer.email,
      document: meliOrder.billingInfo
        ? meliOrder.billingInfo.doc_number
        : '00000000000',
      documentType: meliOrder.billingInfo
        ? meliOrder.billingInfo.doc_type
        : 'OTHER',
      phone: meliOrder.shippingInfo
        ? meliOrder.shippingInfo.receiver_address.receiver_phone
        : null
    },
    discount: 0,
    gross: grossMeli,
    freight: -freight,
    freightClient: -freightClient,
    freightSeller: -freightSeller,
    digiFee: -digiFee,
    saleFee: -sale_fee,
    receivement,
    items: items.map(m => {
      return {
        ...m,
        unit: toFixed(m.unit, 2),
        price: toFixed(m.price, 2),
        gross: toFixed(m.gross, 2),
        total: toFixed(m.total, 2),
        saleFee: toFixed(m.saleFee, 2)
      }
    }),
    payments,
    shipping
  }
}

const getShippingLabel = async (db, config, marketPlace, order) => {
  let sellerColl = db.collection('seller')
  let orderCol = db.collection('order')
  let seller = await sellerColl.findOne({ _id: order.sellerId })

  if (order.shipping.mode == 'me1') {
    let trackingMethod

    switch (order.shipping?.trackingMethod) {
      case 'me1 - expressso' || 'me1 - Expresso' || 'me1 - Prioritário':
        trackingMethod = 'SEDEX'
        break
      case 'me1 - Normal' || 'me1 - normal':
        trackingMethod = 'PAC'
        break
    }

    let orderId = order.packId ? order.packId : order.externalId
    let filter = { transportServiceCode: trackingMethod }

    if (order.shipping.trackingNumber)
      filter.code = order.shipping.trackingNumber

    let labelsColl = db.collection('labels')
    let trackingNumber = await labelsColl.findOneAndUpdate(
      filter,
      { $set: { status: 'indisponivel', orderId } },
      { upsert: true }
    )

    let zplLabel = config.sigep.layoutLabel

    let zplOrder = zplLabel
      .replace('[SIMBOLO_ENCAMINHAMENTO_PACSEDEX_ZPL]')
      .replace('[QRCODE_DATAMATRIZ]', trackingNumber.value.code)
      .replace(
        '[CHANCELA_CORREIOS]',
        config.sigep.transportServiceCode.find(
          method => method.serviceName == trackingMethod
        ).logoService
      )
      .replace('[LOGO_EMPRESA]', config.sigep.logolabeL)
      .replace('Contrato:', `Contrato: ${config.sigep.contrato}`)
      .replace('[NOTAFISCAL_NUMERO]', order.invoice.number)
      .replace('[PEDIDO_NUMERO]', orderId)
      .replace('[ENCOMENDA_NUMERO]', trackingNumber.value.code)
      .replace('[PESO_BRUTO]', 0)
      .replace('[PESO_LIQUIDO]', 0)
      .replace('[VOLUMES_TOTAL]', '1/1')
      .replace(
        '[ENCOMENDA_NUMERO]',
        trackingNumber.value.code.slice(2, trackingNumber.value.code.length - 2)
      )
      .replace('[CONTRATO_NUMERO]', '')
      .replace('[CONTRATO_TIPO]', '')
      .replace('[DESTINATARIO_NOME]', order.buyer.name)
      .replace('[DESTINATARIO_LOGRADOURO]', order.shipping?.address)
      .replace('[DESTINATARIO_COMPLEMENTO]', order.shipping?.comment)
      .replace('[DESTINATARIO_BAIRRO]', order.shipping?.neighborhood)
      .replace('[DESTINATARIO_CIDADE]', order.shipping?.city)
      .replace('[DESTINATARIO_CEP]', order.shipping?.zipCode)
      .replace('[DESTINATARIO_ESTADO]', order.shipping?.state)
      .replace('[REMETENTE_NOME]', seller.name)
      .replace(
        '[REMETENTE_LOGRADOURO]',
        seller.address
          ? seller.address.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          : ''
      )
      .replace(
        '[REMETENTE_BAIRRO]',
        seller.neighborhood
          ? seller.neighborhood.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          : ''
      )
      .replace(
        '[REMETENTE_CIDADE]',
        seller.city
          ? seller.city.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          : ''
      )
      .replace(
        '[REMETENTE_CEP]',
        seller.cep
          ? seller.cep.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          : ''
      )
      .replace(
        '[REMETENTE_ESTADO]',
        seller.state
          ? seller.state.normalize('NFD').replace(/[\u0300-\u036f]/g, '')
          : ''
      )
      .replace(
        'CNPJ/CPF:[DESTINATARIO_CPFCNPJ]',
        seller.document ? seller.document : ''
      )

    await orderCol.updateOne(
      { $or: [{ externalId: order.externalId }, { packId: order.packId }] },
      {
        $set: {
          labelMode: 'S3',
          label: zplOrder,
          'shipping.trackingNumber': trackingNumber.value.code
        }
      }
    )

    return zplOrder
  } else if (!order.label) {
    // caso o contrario verificar qual a plaformar e chamar api correta para buscar etiqueta.
    return (label = await getShippingLabelMeli(
      db,
      config,
      marketPlace,
      order.shipping.shippingId
    ))
  }
}

const sendOrderInvoice = async (
  db,
  user,
  orderId,
  key,
  number,
  serie,
  emissionDate,
  xml
) => {
  let orderCollection = db.collection('order')
  let configCollection = db.collection('config')
  let marketPlaceCollection = db.collection('marketPlace')
  let platformCollection = db.collection('platform')

  let filterOrder = {
    sellerId: { $in: user.sellerIds },
    $or: [{ externalId: parseFloat(orderId) }, { packId: parseFloat(orderId) }]
  }
  let order = await orderCollection.findOne(filterOrder)

  if (!order) throw `Order ${orderId} not found to this user`
  if (!order.shipping) throw `Order doesn't have a shipping`

  let config = await configCollection.findOne({})
  let marketPlace = await marketPlaceCollection.findOne({
    _id: order.marketPlaceId
  })
  let platform = await platformCollection.findOne({
    _id: marketPlace.platformId
  })

  let label
  let invoice = {
    key,
    number,
    serie,
    emissionDate: new Date(emissionDate),
    xml
  }
  order.invoice = invoice

  let error = []

  switch (platform.code) {
    case 'MLB':
      let shipping = await getShippingById(
        db,
        config,
        marketPlace,
        order.shipping.shippingId
      )

      if (shipping.substatus == 'invoice_pending') {
        await postShippingInvoice(
          db,
          config,
          marketPlace,
          order.shipping.shippingId,
          xml
        )
      }

      // tenta imprimir a etiqueta até 5x o meli tem um delay para gerar a etiqueta
      for (let i = 1; i <= 5; i++) {
        try {
          label = await getShippingLabel(db, config, marketPlace, order)
        } catch (e) {
          if (i == 5) error.push(e)
        }

        if (label) break
        await sleep(5000)
      }

      break
  }
  let labelUpdate = {}

  if (label) {
    let pathLabel = await uploadFileS3(
      label,
      `${order.packId ? order.packId : order.externalId}.txt`
    )
    labelUpdate.label = pathLabel
    labelUpdate.labelMode = 'S3'
  }

  let pathNf = await uploadFileS3(Buffer.from(xml, 'utf-8'), `${key}.xml`)

  labelUpdate.status = 'invoiced'
  labelUpdate.updatedAt = new Date()
  invoice.xml = pathNf
  invoice.mode = 'pathNf'

  await orderCollection.updateOne(
    {
      $or: [
        { externalId: parseFloat(orderId) },
        { packId: parseFloat(orderId) }
      ],
      sellerId: { $in: user.sellerIds }
    },
    { $set: { invoice, ...labelUpdate } }
  )

  if (error[0]) throw error[0]

  return label
}

const getInvoicedMeliFulfillment = async (db, data, config) => {
  let order = data
  let marketPlaceColl = db.collection('marketPlace')
  let marketPlace = await marketPlaceColl
    .find({ _id: data.marketPlaceId })
    .toArray()

  if (order.shipping.fulfillment == true) {
    let invoice = {}

    let ret = await getInvoiceMeli(db, config, marketPlace, data)
    let xml = await getXmlMeli(db, config, marketPlace, ret.id)
    let pathNf = await uploadFileS3(
      Buffer.from(xml, 'utf-8'),
      `${ret.attributes.invoice_key}.xml`
    )

    invoice['serie'] = ret.invoice_series
    invoice['number'] = ret.invoice_number
    invoice['key'] = ret.attributes.invoice_key
    invoice['emissionDate'] = new Date(ret.attributes.authorization_date)
    invoice['xml'] = pathNf

    return invoice
  }
}

module.exports = {
  meliOrderToDigigrowOrder,
  sendOrderInvoice,
  getShippingLabel,
  getInvoicedMeliFulfillment
}
