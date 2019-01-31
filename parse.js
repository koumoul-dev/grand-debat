const fs = require('fs-extra')
const path = require('path')
const ndjson = require('ndjson')
const { Transform } = require('stream')
const endOfLine = require('os').EOL

const files = fs.readdirSync(path.join(__dirname, 'data')).filter(f => f.split('.').pop() === 'json')
const streamToPromise = require('stream-to-promise')

const fields = ['publishedAt', 'titre', 'id', 'reference', 'url', 'createdAt', 'updatedAt', 'author_id', 'question_id', 'question', 'response']
fs.ensureDirSync(path.join(__dirname, 'out'))

const build = async (file) => {
  const transform = new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      const {responses, ...common} = item
      responses.forEach(r => {
        try {
          const parsed = JSON.parse(r.value)
          this.push(Object.assign({}, common, {
            question_id: r.question.id,
            question: r.question.title,
            response: parsed.labels[0]
          }))
        }catch(e){
          this.push(Object.assign({}, common, {
            question_id: r.question.id,
            question: r.question.title,
            response: r.value
          }))
        }
      })
      callback()
    }
  })

  const filter = new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      if(item.response){
        callback(null, item)
      } else callback(null)
    }
  })

  const join =  new Transform({
    objectMode: true,
    transform(item, encoding, callback) {
      callback(null, fields.map(f => item[f] ? `"${item[f].replace(/"/g, '""')}"` : '').join(',') + endOfLine)
    }
  })

  console.log(file)
  const writeStream = fs.createWriteStream(path.join(__dirname, 'out', 'grand-debat-'+file.split('.').shift() + '.csv'))
  writeStream.write(fields.map(f => `"${f}"`).join(',') + endOfLine)
  fs.createReadStream(path.join(__dirname, 'data', file))
    .pipe(ndjson.parse())
    .pipe(transform)
    .pipe(filter)
    .pipe(join)
    .pipe(writeStream)
  return await streamToPromise(writeStream)
}

async function run(){
  for (const file of files) {
    await build(file)
  }
}

run()
