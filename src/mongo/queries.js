# Last date of speech per politician
db.politicians.aggregate([
    {$unwind: "$speeches"},
    {$group: {_id: '$name', last_speech: {$max: "$speeches.date"}}}
])