<?php

namespace EasyRdf\Sparql\Response;

use EasyRdf\Exception;
use EasyRdf\Http\Response;

/**
 * Works with Virtuoso 6 and 7
 */
class JsonLD
{
    /**
     * Default graph uri
     *
     * @var string
     */
    private $graphUri;

    /**
     * Values for each property
     *
     * @var array
     */
    private $valueList;

    /**
     * Property type
     *
     * @var string
     */
    private $type;

    /**
     * @param Response $response
     */
    public function __construct(Response $response)
    {
        return $this->getData($response);
    }

    /**
     * @return string
     */
    public function getGraphUri()
    {
        return $this->graphUri;
    }

    /**
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @return array
     */
    public function getValueList()
    {
        return $this->valueList;
    }

    /**
     * Return an object that contains the graph, property and values from the sparql response
     *
     * @param Response $response
     *
     * @return JsonLD
     * @throws Exception Cannot parse Body
     */
    protected function getData(Response $response)
    {
        $responseBody = $response->getBody();
        $this->graphUri = $response->getHeader('X-sparql-default-graph');

        $body = json_decode($responseBody);

        if (!is_object($body)) {
            throw new Exception('Cannot parse Body');
        }

        if (isset($body->{'@graph'})) {
            $dataObject = current($body->{'@graph'});
        } elseif ($body->{'@'}) {
            $dataObject = current($body->{'@'});
        } else {

            return $this;
        }
        $dataList = get_object_vars($dataObject);
        foreach ($dataList as $key => $data) {
            if ($key === '@type' || $key === 'a') {
                $this->type = $data;
            } elseif (\is_array($data)) {
                $this->setValueList($key, $data);
            }
        }

        return $this;
    }

    /**
     * set values for each property
     *
     * @param string $property
     * @param array $data
     */
    protected function setValueList($property, array $data)
    {
        if (count($data) === 1) {
            $value = current($data);
            if (is_object($value)) {
                $this->valueList[$property] = current(get_object_vars($value));
            } else {
                $this->valueList[$property] = $value;
            }
        } else {
            foreach ($data as $value) {
                if (is_object($value)) {
                    $this->valueList[$property][] = current(get_object_vars($value));
                } else {
                    $this->valueList[$property][] = $value;
                }
            }
        }
    }
}