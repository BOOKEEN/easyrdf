<?php

namespace EasyRdf\Sparql\Response;

use EasyRdf\Http\Response;

class JsonLd
{

    /**
     * @var \stdClass
     */
    private $jsonLd;

    /**
     * @param Response $response
     */
    public function __construct(Response $response)
    {
        $this->jsonLd = $this->normalizeJsonLd($response);
    }

    /**
     * @return \stdClass
     */
    public function getJsonLd()
    {
        return $this->jsonLd;
    }

    /**
     * Normalize Json LD
     * (currently works with virtuoso 6 Json LD format)
     *
     * @param Response $response
     *
     * @return \stdClass
     */
    protected function normalizeJsonLd(Response $response)
    {
        $responseBody = $response->getBody();
        $bodyContent = json_decode($responseBody, true);

        if (empty($bodyContent)) {

            return new \stdClass();
        }

        if (isset($bodyContent['@graph'])) {

            return json_decode($responseBody);
        } elseif (isset($bodyContent['@'])) {
            $jsonLd = new \stdClass();
            $jsonLd->{'@graph'} = array();
            foreach ($bodyContent['@'] as $graph) {
                $newNode = new \stdClass();
                foreach ($graph as $subject => $node) {
                    if ($subject === '@') {
                        $newNode->{'@id'} = $node;
                    } elseif ($subject === 'a') {
                        $newNode->{'@type'} = $node;
                    } else {
                        $this->getNodeValue($node, $subject, $newNode);
                    }
                }
                $jsonLd->{'@graph'}[] = $newNode;
            }

            return $jsonLd;
        } else {

            return new \stdClass();
        }
    }

    /**
     * @param array $node
     * @param string $subject
     * @param \stdClass $newNode
     */
    private function getNodeValue($node, $subject, &$newNode)
    {
        foreach ($node as $value) {
            if (\is_int($value) || \is_bool($value)) {
                $newNode->$subject = $value;
            } elseif (\is_string($value)) {
                $newSubject = new \stdClass();
                $newSubject->{'@id'} = $value;
                $newNode->$subject = $newSubject;
            } elseif (\is_array($value)) {
                $newSubject = new \stdClass();
                if (isset($value['@literal'])) {
                    $newSubject->{'@value'} = $value['@literal'];
                }
                if (isset($value['@language'])) {
                    $newSubject->{'@language'} = $value['@language'];
                }
                $newNode->$subject = $newSubject;
            }
        }
    }
}